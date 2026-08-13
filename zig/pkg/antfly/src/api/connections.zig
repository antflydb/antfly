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

// GET /connections — configured external connections.
//
// Enumerates configured inference providers, external IO endpoints, and CDC
// sources from the public node-config `connections` map. With the "models"
// expansion each inference provider's list-models API is queried live;
// per-connection failures degrade to status "error" without failing the
// response.

const std = @import("std");
const httpx = @import("httpx");
const objectstore = @import("objectstore");
const platform_time = @import("antfly_platform").time;
const platform_sync = @import("antfly_platform").sync;
const common_config = @import("../common/config.zig");
const common_secrets = @import("../common/secrets.zig");
const metadata_api = @import("../metadata/api.zig");
const bedrock = @import("../inference/bedrock.zig");
const list_models = @import("../inference/list_models.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const backups_api = @import("backups.zig");

const Allocator = std.mem.Allocator;

pub const ConnectionKind = enum {
    inference,
    web_search,
    external_io,
    cdc,
};

pub const ConnectionStatus = enum {
    connected,
    @"error",
    configured,
    unsupported,
};

pub const ConfiguredModelType = enum {
    embedder,
    generator,
    reranker,
    chunker,
};

pub const ConnectedModel = struct {
    name: []const u8,
    display_name: ?[]const u8 = null,
    dimensions: ?u32 = null,
    configured: ?bool = null,
};

pub const InferenceConnection = struct {
    provider: list_models.ProviderTag,
    url: ?[]const u8 = null,
    region: ?[]const u8 = null,
    project_id: ?[]const u8 = null,
    location: ?[]const u8 = null,
    names: []const []const u8 = &.{},
    configured_model_types: []const []const u8 = &.{},
    models: ?std.json.ArrayHashMap([]const ConnectedModel) = null,
};

pub const WebSearchConnection = struct {
    service: ?[]const u8 = null,
    max_results: ?u32 = null,
    timeout_ms: ?u32 = null,
    safe_search: ?bool = null,
    language: ?[]const u8 = null,
    region: ?[]const u8 = null,
    include_content: ?bool = null,
    include_highlights: ?bool = null,
    endpoint: ?[]const u8 = null,
    project_id: ?[]const u8 = null,
    location: ?[]const u8 = null,
    data_store: ?[]const u8 = null,
    serving_config: ?[]const u8 = null,
    include_domains: []const []const u8 = &.{},
    exclude_domains: []const []const u8 = &.{},
    configured: ?bool = null,
};

pub const ExternalIoProtocol = enum {
    s3,
    gcs,
    filesystem,
    http,
};

pub const ExternalIoConnection = struct {
    protocol: ExternalIoProtocol,
    endpoint: ?[]const u8 = null,
    buckets: []const []const u8 = &.{},
    prefix: ?[]const u8 = null,
    hosts: []const []const u8 = &.{},
};

pub const CdcConnection = struct {
    provider: []const u8,
    table_name: []const u8,
    source_ordinal: u32,
    external_table: ?[]const u8 = null,
    slot_name: ?[]const u8 = null,
    publication_name: ?[]const u8 = null,
    phase: ?[]const u8 = null,
    lag_records: ?u64 = null,
    lag_millis: ?u64 = null,
    last_success_at_ms: ?u64 = null,
    last_change_applied_at_ms: ?u64 = null,
    updated_at_ms: ?u64 = null,
};

pub const Connection = struct {
    id: []const u8,
    name: []const u8,
    display_name: ?[]const u8 = null,
    provider: ?[]const u8 = null,
    kind: ConnectionKind,
    status: ConnectionStatus,
    @"error": ?[]const u8 = null,
    capabilities: []const []const u8 = &.{},
    sources: []const []const u8 = &.{},
    inference: ?InferenceConnection = null,
    web_search: ?WebSearchConnection = null,
    external_io: ?ExternalIoConnection = null,
    cdc: ?CdcConnection = null,
};

pub const ConnectionsResponse = struct {
    connections: []const Connection = &.{},
};

pub const InvokeResult = struct {
    status: u16,
    body: []u8,
    retry_after: ?[]u8 = null,

    pub fn deinit(self: *InvokeResult, alloc: Allocator) void {
        alloc.free(self.body);
        if (self.retry_after) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const inference_retry_after_seconds = "1";

/// Stable public payload for caller-side inference admission failures. Keep
/// this synchronized with TransientCapacityError in the inference API spec.
pub const InferenceAdmissionFailure = struct {
    @"error": []const u8 = "SERVICE_UNAVAILABLE",
    message: []const u8 = "inference capacity exhausted",
    reason: []const u8 = "inference_admission",
    retryable: bool = true,
    retry_after_ms: u32 = 1000,
};

pub fn inferenceAdmissionFailure() InferenceAdmissionFailure {
    return .{};
}

pub const InferenceAdmissionOwner = enum {
    caller,
    target,
};

const ResolvedInferenceConnection = struct {
    url: []const u8,
    admission_owner: InferenceAdmissionOwner,
};

/// Resolve the forwarding target before reading the request body. A target in
/// this process owns its route admission; remote targets are admitted by the
/// forwarding API for the full upstream request.
pub fn inferenceConnectionAdmissionOwner(
    node_config: ?*const common_config.Config,
    local_inference_api_url: ?[]const u8,
    connection_id: []const u8,
    operation: []const u8,
) !InferenceAdmissionOwner {
    return (try resolveInferenceConnection(node_config, local_inference_api_url, connection_id, operation)).admission_owner;
}

pub fn invokeInferenceConnection(
    alloc: Allocator,
    http: *httpx.Client,
    node_config: ?*const common_config.Config,
    local_inference_api_url: ?[]const u8,
    local_inference_api_key: ?[]const u8,
    secret_store: ?*common_secrets.FileStore,
    connection_id: []const u8,
    operation: []const u8,
    body: []const u8,
) !InvokeResult {
    const resolved = try resolveInferenceConnection(node_config, local_inference_api_url, connection_id, operation);
    const raw_url = resolved.url;
    const base = std.mem.trimEnd(u8, raw_url, "/");
    const prefix = if (std.mem.endsWith(u8, base, "/ai/v1")) base else try std.fmt.allocPrint(alloc, "{s}/ai/v1", .{base});
    defer if (prefix.ptr != base.ptr) alloc.free(prefix);
    const url = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ prefix, operation });
    defer alloc.free(url);

    const configured_api_key = if (std.mem.eql(u8, connection_id, common_config.local_inference_connection_id))
        local_inference_api_key
    else
        node_config.?.connections.get(connection_id).?.inference.?.api_key;
    const resolved_key = try resolveInferenceApiKey(alloc, secret_store, configured_api_key);
    defer if (resolved_key) |key| alloc.free(key);
    const auth = if (resolved_key) |key| try std.fmt.allocPrint(alloc, "Bearer {s}", .{key}) else null;
    defer if (auth) |value| alloc.free(value);
    const headers = if (auth) |value| &[_][2][]const u8{
        .{ "Authorization", value },
        .{ "Content-Type", "application/json" },
    } else &[_][2][]const u8{.{ "Content-Type", "application/json" }};

    var response = try http.request(.POST, url, .{ .headers = headers, .body = body, .timeout_ms = 120_000 });
    defer response.deinit();
    const response_body = try alloc.dupe(u8, response.body orelse "");
    errdefer alloc.free(response_body);
    const retry_after = if (response.header("Retry-After")) |value|
        try alloc.dupe(u8, value)
    else
        null;
    return .{
        .status = response.status.code,
        .body = response_body,
        .retry_after = retry_after,
    };
}

fn resolveInferenceConnection(
    node_config: ?*const common_config.Config,
    local_inference_api_url: ?[]const u8,
    connection_id: []const u8,
    operation: []const u8,
) !ResolvedInferenceConnection {
    const required_capability = inferenceCapability(operation) orelse return error.UnsupportedInferenceOperation;
    if (std.mem.eql(u8, connection_id, common_config.local_inference_connection_id)) {
        const local_url = local_inference_api_url orelse return error.ConnectionNotFound;
        if (!validInferenceURL(local_url)) return error.InvalidConnectionURL;
        return .{
            .url = local_url,
            .admission_owner = .target,
        };
    }

    const config = node_config orelse return error.ConnectionNotFound;
    const connection = config.connections.get(connection_id) orelse return error.ConnectionNotFound;
    if (connection.kind != .inference) return error.ConnectionNotInference;
    try requireInferenceCapability(connection.capabilities, required_capability);
    const cfg = connection.inference orelse return error.InvalidConfig;
    if (!std.mem.eql(u8, cfg.provider, "antfly")) return error.ProviderNotAntflyCompatible;
    const raw_url = cfg.url orelse return error.ConnectionURLMissing;
    if (!validInferenceURL(raw_url)) return error.InvalidConnectionURL;
    return .{
        .url = raw_url,
        .admission_owner = if (local_inference_api_url) |local_url|
            if (sameInferenceBaseURL(raw_url, local_url)) .target else .caller
        else
            .caller,
    };
}

fn sameInferenceBaseURL(first: []const u8, second: []const u8) bool {
    return std.ascii.eqlIgnoreCase(trimInferenceBaseURL(first), trimInferenceBaseURL(second));
}

fn trimInferenceBaseURL(raw: []const u8) []const u8 {
    var value = std.mem.trimEnd(u8, raw, "/");
    if (std.mem.endsWith(u8, value, "/ai/v1")) value = value[0 .. value.len - "/ai/v1".len];
    return std.mem.trimEnd(u8, value, "/");
}

fn inferenceCapability(operation: []const u8) ?[]const u8 {
    inline for (&.{
        .{ "embed", "models.embed" },
        .{ "generate", "models.generate" },
        .{ "rerank", "models.rerank" },
        .{ "chunk", "models.chunk" },
        .{ "extract", "models.extract" },
        .{ "rewrite", "models.rewrite" },
        .{ "read", "models.read" },
        .{ "transcribe", "models.transcribe" },
    }) |mapping| {
        if (std.mem.eql(u8, operation, mapping[0])) return mapping[1];
    }
    return null;
}

fn resolveInferenceApiKey(
    alloc: Allocator,
    secret_store: ?*common_secrets.FileStore,
    raw: ?[]const u8,
) !?[]u8 {
    return if (raw) |value| try common_secrets.resolveReferenceOwned(alloc, secret_store, value) else null;
}

fn requireInferenceCapability(capabilities: []const []const u8, required: []const u8) !void {
    if (!containsString(capabilities, required)) return error.ConnectionCapabilityMissing;
}

fn validInferenceURL(raw_url: []const u8) bool {
    if (std.mem.indexOfAny(u8, raw_url, "?#") != null) return false;
    const parsed = std.Uri.parse(raw_url) catch return false;
    return parsed.host != null and
        (std.ascii.eqlIgnoreCase(parsed.scheme, "http") or std.ascii.eqlIgnoreCase(parsed.scheme, "https"));
}

pub const Sources = struct {
    node_config: ?*const common_config.Config = null,
    snapshot: ?*const metadata_api.AdminSnapshot = null,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    inference_api_url: ?[]const u8 = null,
    inference_api_key: ?[]const u8 = null,
    /// Live secret source used to resolve external-I/O credential references
    /// immediately before a probe.
    secret_store: ?*common_secrets.FileStore = null,
    /// Shared server runtime for bounded connection discovery fanout. Tests
    /// and embedded callers without a runtime fall back to sequential work.
    io: ?std.Io = null,
};

pub const BuildOptions = struct {
    include_models: bool = false,
    refresh: bool = false,
    cached_err_name: ?[]const u8 = null,
    probe: bool = false,
    timeout_ms: u64 = 5_000,
    max_workers: usize = 8,
    ttl_ns: u64 = 30 * std.time.ns_per_s,
    /// Raw comma-separated kind filter from the `types` query param.
    types_filter: ?[]const u8 = null,
};

/// Short-lived per-connection result cache so dashboards do not hammer
/// provider APIs. Keyed by connection identity; entries are owned by the
/// cache allocator.
pub const Cache = struct {
    const key_lock_count = 64;
    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    key_locks: [key_lock_count]std.Io.Mutex = [_]std.Io.Mutex{.init} ** key_lock_count,
    entries: std.StringArrayHashMapUnmanaged(Entry) = .{},

    pub const Entry = struct {
        captured_at_ns: u64,
        ok: bool,
        err_name: []u8 = &.{},
        models: []list_models.ListedModel = &.{},

        fn deinitOwned(self: *Entry, alloc: Allocator) void {
            if (self.err_name.len > 0) alloc.free(self.err_name);
            for (self.models) |*model| {
                alloc.free(model.name);
                if (model.display_name) |value| alloc.free(value);
            }
            if (self.models.len > 0) alloc.free(self.models);
            self.* = undefined;
        }
    };

    pub fn init(alloc: Allocator) Cache {
        return .{ .alloc = alloc };
    }

    fn lock(self: *Cache) void {
        platform_sync.lockYielding(&self.mutex);
    }

    fn unlock(self: *Cache) void {
        self.mutex.unlock();
    }

    fn keyLock(self: *Cache, key: []const u8) *std.Io.Mutex {
        return &self.key_locks[std.hash.Wyhash.hash(0, key) % key_lock_count];
    }

    pub fn deinit(self: *Cache) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            entry.value_ptr.deinitOwned(self.alloc);
        }
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    /// Deep-copy a fresh entry into `arena`. Returns null on miss or expiry.
    fn lookupCopy(self: *Cache, arena: Allocator, key: []const u8, now_ns: u64, ttl_ns: u64) !?Entry {
        self.lock();
        defer self.unlock();
        const entry = self.entries.get(key) orelse return null;
        if (now_ns -| entry.captured_at_ns > ttl_ns) return null;
        return try copyEntry(arena, entry);
    }

    fn store(self: *Cache, key: []const u8, entry: Entry) !void {
        self.lock();
        defer self.unlock();
        var owned = try copyEntry(self.alloc, entry);
        errdefer owned.deinitOwned(self.alloc);
        if (self.entries.getPtr(key)) |existing| {
            existing.deinitOwned(self.alloc);
            existing.* = owned;
            return;
        }
        // Duplicate the key before mutating the map. `getOrPut` inserts the
        // caller-owned key immediately, which would leave a dangling key and
        // uninitialized value if the subsequent duplication failed.
        const owned_key = try self.alloc.dupe(u8, key);
        errdefer self.alloc.free(owned_key);
        try self.entries.putNoClobber(self.alloc, owned_key, owned);
    }

    fn copyEntry(alloc: Allocator, entry: Entry) !Entry {
        const models = try alloc.alloc(list_models.ListedModel, entry.models.len);
        var copied: usize = 0;
        errdefer {
            for (models[0..copied]) |*model| {
                alloc.free(model.name);
                if (model.display_name) |value| alloc.free(value);
            }
            alloc.free(models);
        }
        for (entry.models, 0..) |model, i| {
            models[i] = .{
                .name = try alloc.dupe(u8, model.name),
                .display_name = if (model.display_name) |value| try alloc.dupe(u8, value) else null,
                .kind = model.kind,
                .dimensions = model.dimensions,
            };
            copied = i + 1;
        }
        return .{
            .captured_at_ns = entry.captured_at_ns,
            .ok = entry.ok,
            .err_name = if (entry.err_name.len > 0) try alloc.dupe(u8, entry.err_name) else &.{},
            .models = models,
        };
    }
};

/// Deduped inference provider instance gathered from configs.
const Instance = struct {
    provider: list_models.ProviderTag,
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    region: []const u8 = "",
    project_id: []const u8 = "",
    location: []const u8 = "",
    credentials_path: []const u8 = "",
    key: []const u8 = "",
    names: std.ArrayListUnmanaged([]const u8) = .empty,
    sources: std.ArrayListUnmanaged([]const u8) = .empty,
    model_types: std.EnumSet(ConfiguredModelType) = std.EnumSet(ConfiguredModelType).initEmpty(),
    configured_models: std.StringArrayHashMapUnmanaged(void) = .{},
};

fn containsString(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

const ModelsOutcome = struct {
    ok: bool,
    err_name: []const u8 = "",
    models: []list_models.ListedModel = &.{},
};

const ModelsJob = struct {
    ep: list_models.Endpoint,
    timeout_ms: u64,
    arena_state: std.heap.ArenaAllocator,
    result: ?list_models.ListResult = null,
    err: ?anyerror = null,
    cache: ?*Cache = null,
    cache_key: []const u8 = "",
    request_started_ns: u64 = 0,
    ttl_ns: u64 = 0,
    refresh: bool = false,
    cached_err_name: ?[]const u8 = null,
    io: ?std.Io = null,

    fn run(job: *ModelsJob) std.Io.Cancelable!void {
        const alloc = job.arena_state.allocator();
        const key_lock = if (job.io) |io|
            if (job.cache) |cache| .{ .lock = cache.keyLock(job.cache_key), .io = io } else null
        else
            null;
        if (key_lock) |guard| guard.lock.lockUncancelable(guard.io);
        defer if (key_lock) |guard| guard.lock.unlock(guard.io);
        if (job.cache) |cache| {
            const now = platform_time.monotonicNs();
            if (cache.lookupCopy(alloc, job.cache_key, now, job.ttl_ns) catch null) |entry| {
                if (!job.refresh or entry.captured_at_ns >= job.request_started_ns) {
                    if (entry.ok) {
                        job.result = .{ .models = entry.models };
                    } else {
                        job.err = error.CachedProviderFailure;
                        job.cached_err_name = entry.err_name;
                    }
                    return;
                }
            }
        }
        var fallback_io_impl: ?std.Io.Threaded = if (job.io == null) std.Io.Threaded.init(std.heap.page_allocator, .{}) else null;
        defer if (fallback_io_impl) |*io_impl| io_impl.deinit();
        const io = job.io orelse fallback_io_impl.?.io();
        var client = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false });
        defer client.deinit();
        job.result = list_models.listModels(alloc, &client, job.ep, job.timeout_ms) catch |err| {
            job.err = err;
            if (job.cache) |cache| cache.store(job.cache_key, .{
                .captured_at_ns = platform_time.monotonicNs(),
                .ok = false,
                .err_name = @constCast(@errorName(err)),
            }) catch {};
            return;
        };
        if (job.cache) |cache| cache.store(job.cache_key, .{
            .captured_at_ns = platform_time.monotonicNs(),
            .ok = true,
            .models = job.result.?.models,
        }) catch {};
    }
};

/// Build the connections response. All returned memory is owned by `arena`.
pub fn buildConnectionsResponse(
    arena: Allocator,
    sources: Sources,
    cache: ?*Cache,
    opts: BuildOptions,
) !ConnectionsResponse {
    var kinds = std.EnumSet(ConnectionKind).initFull();
    if (opts.types_filter) |filter| kinds = parseKindFilter(filter);
    const effective_opts = opts;

    var connections = std.ArrayListUnmanaged(Connection).empty;

    if (kinds.contains(.inference) and sources.antfly_provider != null) {
        try appendLocalInferenceConnection(arena, &connections, sources, cache, effective_opts);
    }

    if (sources.node_config) |node_config| {
        try appendConfiguredConnections(arena, &connections, sources, cache, effective_opts, kinds, node_config);
        if (effective_opts.probe and kinds.contains(.external_io)) {
            try resolveExternalIoProbes(arena, &connections, node_config, cache, effective_opts, sources.io, sources.secret_store);
        }
    }

    return .{ .connections = connections.items };
}

fn appendLocalInferenceConnection(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    sources: Sources,
    cache: ?*Cache,
    opts: BuildOptions,
) !void {
    const instance = try arena.create(Instance);
    instance.* = .{
        .provider = .antfly,
        .key = "runtime:local-inference",
    };
    try instance.sources.append(arena, "runtime:local-inference");

    var connection = Connection{
        .id = common_config.local_inference_connection_id,
        .name = common_config.local_inference_connection_id,
        .display_name = "Local inference",
        .kind = .inference,
        .status = .configured,
        .capabilities = &.{ "embedding", "generation", "reranking", "chunking" },
        .sources = &.{"runtime:local-inference"},
        .inference = .{ .provider = .antfly },
    };

    if (opts.include_models) {
        const instances = try arena.alloc(*Instance, 1);
        instances[0] = instance;
        const outcomes = try resolveModels(arena, sources, cache, opts, instances);
        if (outcomes[0]) |outcome| {
            if (outcome.ok) {
                connection.status = .connected;
                connection.inference.?.models = try modelsMapAlloc(arena, outcome.models, instance);
            } else {
                connection.status = .@"error";
                connection.@"error" = outcome.err_name;
            }
        }
    }

    try connections.append(arena, connection);
}

fn parseKindFilter(filter: []const u8) std.EnumSet(ConnectionKind) {
    var kinds = std.EnumSet(ConnectionKind).initEmpty();
    var it = std.mem.splitScalar(u8, filter, ',');
    while (it.next()) |raw| {
        const trimmed = std.mem.trim(u8, raw, " \t");
        if (std.meta.stringToEnum(ConnectionKind, trimmed)) |kind| {
            kinds.insert(kind);
        }
    }
    return kinds;
}

pub fn includeHasModels(include: ?[]const u8) bool {
    const raw = include orelse return false;
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |value| {
        if (std.mem.eql(u8, std.mem.trim(u8, value, " \t"), "models")) return true;
    }
    return false;
}

pub fn includeHasStatus(include: ?[]const u8) bool {
    const raw = include orelse return false;
    var it = std.mem.splitScalar(u8, raw, ',');
    while (it.next()) |value| {
        if (std.mem.eql(u8, std.mem.trim(u8, value, " \t"), "status")) return true;
    }
    return false;
}

fn appendConfiguredConnections(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    sources: Sources,
    cache: ?*Cache,
    opts: BuildOptions,
    kinds: std.EnumSet(ConnectionKind),
    node_config: *const common_config.Config,
) !void {
    var it = node_config.connections.iterator();
    while (it.next()) |entry| {
        const cfg = entry.value_ptr.*;
        const kind = connectionKindFromConfig(cfg.kind);
        if (!kinds.contains(kind)) continue;

        switch (kind) {
            .inference => try appendConfiguredInferenceConnection(
                arena,
                connections,
                sources,
                cache,
                opts,
                entry.key_ptr.*,
                cfg,
            ),
            .web_search => try appendConfiguredWebSearchConnection(
                arena,
                connections,
                entry.key_ptr.*,
                cfg,
            ),
            .external_io => try appendConfiguredExternalIoConnection(
                arena,
                connections,
                entry.key_ptr.*,
                cfg,
            ),
            .cdc => try appendConfiguredCdcConnection(
                arena,
                connections,
                sources.snapshot,
                entry.key_ptr.*,
                cfg,
            ),
        }
    }
}

fn appendConfiguredInferenceConnection(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    sources: Sources,
    cache: ?*Cache,
    opts: BuildOptions,
    id: []const u8,
    cfg: common_config.Config.ConnectionConfig,
) !void {
    const inference_cfg = cfg.inference orelse return error.InvalidConfig;
    const provider = std.meta.stringToEnum(list_models.ProviderTag, inference_cfg.provider) orelse return error.InvalidConfig;
    const model_types = try configuredModelTypeSet(inference_cfg.configured_model_types);
    const instance = try instanceFromConnectionConfig(arena, id, provider, inference_cfg, model_types);

    var connection = Connection{
        .id = id,
        .name = id,
        .display_name = cfg.display_name,
        .kind = .inference,
        .status = if (provider == .mock) .connected else .configured,
        .capabilities = cfg.capabilities,
        .sources = try sourcesSlice(arena, "config:connections/{s}", id),
        .inference = .{
            .provider = provider,
            .url = inference_cfg.url,
            .region = inference_cfg.region,
            .project_id = inference_cfg.project_id,
            .location = inference_cfg.location,
            .names = inference_cfg.names,
            .configured_model_types = inference_cfg.configured_model_types,
        },
    };

    if (opts.include_models) {
        const instances = try arena.alloc(*Instance, 1);
        instances[0] = instance;
        const outcomes = try resolveModels(arena, sources, cache, opts, instances);
        if (outcomes[0]) |outcome| {
            if (outcome.ok) {
                connection.status = .connected;
                connection.inference.?.models = try modelsMapAlloc(arena, outcome.models, instance);
            } else {
                connection.status = .@"error";
                connection.@"error" = outcome.err_name;
            }
        }
    }

    try connections.append(arena, connection);
}

fn appendConfiguredWebSearchConnection(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    id: []const u8,
    cfg: common_config.Config.ConnectionConfig,
) !void {
    const provider = cfg.provider orelse return error.InvalidConfig;
    const web_search_cfg = cfg.web_search orelse return error.InvalidConfig;
    const connection = Connection{
        .id = id,
        .name = id,
        .display_name = cfg.display_name,
        .provider = provider,
        .kind = .web_search,
        .status = if (isKnownWebSearchProvider(provider)) .configured else .unsupported,
        .capabilities = cfg.capabilities,
        .sources = try sourcesSlice(arena, "config:connections/{s}", id),
        .web_search = .{
            .service = web_search_cfg.service,
            .max_results = web_search_cfg.max_results,
            .timeout_ms = web_search_cfg.timeout_ms,
            .safe_search = web_search_cfg.safe_search,
            .language = web_search_cfg.language,
            .region = web_search_cfg.region,
            .include_content = web_search_cfg.include_content,
            .include_highlights = web_search_cfg.include_highlights,
            .endpoint = web_search_cfg.endpoint,
            .project_id = web_search_cfg.project_id,
            .location = web_search_cfg.location,
            .data_store = web_search_cfg.data_store,
            .serving_config = web_search_cfg.serving_config,
            .include_domains = web_search_cfg.include_domains,
            .exclude_domains = web_search_cfg.exclude_domains,
            .configured = webSearchConfigured(provider, web_search_cfg),
        },
    };

    try connections.append(arena, connection);
}

fn appendConfiguredExternalIoConnection(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    id: []const u8,
    cfg: common_config.Config.ConnectionConfig,
) !void {
    const external_cfg = cfg.external_io orelse return error.InvalidConfig;
    const connection = Connection{
        .id = id,
        .name = id,
        .display_name = cfg.display_name,
        .kind = .external_io,
        .status = .configured,
        .capabilities = cfg.capabilities,
        .sources = try sourcesSlice(arena, "config:connections/{s}", id),
        .external_io = .{
            .protocol = externalIoProtocolFromConfig(external_cfg.protocol),
            .endpoint = external_cfg.endpoint,
            .buckets = external_cfg.buckets,
            .prefix = external_cfg.prefix,
            .hosts = external_cfg.hosts,
        },
    };

    try connections.append(arena, connection);
}

fn appendConfiguredCdcConnection(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    snapshot: ?*const metadata_api.AdminSnapshot,
    id: []const u8,
    cfg: common_config.Config.ConnectionConfig,
) !void {
    const cdc_cfg = cfg.cdc orelse return error.InvalidConfig;
    const table_name = cdc_cfg.table_name orelse return error.InvalidConfig;
    const source_ordinal = cdc_cfg.source_ordinal orelse return error.InvalidConfig;
    const status = if (snapshot) |snap| blk: {
        const table = findTableByName(snap, table_name) orelse break :blk null;
        break :blk findReplicationSourceStatus(snap, table.table_id, source_ordinal);
    } else null;

    var connection = Connection{
        .id = id,
        .name = id,
        .display_name = cfg.display_name,
        .kind = .cdc,
        .status = cdcStatusFromReplicationStatus(status),
        .capabilities = cfg.capabilities,
        .sources = try sourcesSlice(arena, "config:connections/{s}", id),
        .cdc = .{
            .provider = cdc_cfg.provider,
            .table_name = table_name,
            .source_ordinal = source_ordinal,
            .external_table = if (status) |record| nonEmpty(record.external_table) else cdc_cfg.external_table,
            .slot_name = if (status) |record| nonEmpty(record.slot_name) else cdc_cfg.slot_name,
            .publication_name = if (status) |record| nonEmpty(record.publication_name) else cdc_cfg.publication_name,
            .phase = if (status) |record| nonEmpty(record.phase) else null,
            .lag_records = if (status) |record| record.lag_records else null,
            .lag_millis = if (status) |record| record.lag_millis else null,
            .last_success_at_ms = if (status) |record| nonZero(record.last_success_at_ms) else null,
            .last_change_applied_at_ms = if (status) |record| nonZero(record.last_change_applied_at_ms) else null,
            .updated_at_ms = if (status) |record| nonZero(record.updated_at_ms) else null,
        },
    };
    if (status) |record| {
        if (record.last_error.len > 0) connection.@"error" = record.last_error;
    }

    try connections.append(arena, connection);
}

fn connectionKindFromConfig(kind: common_config.Config.ConnectionKind) ConnectionKind {
    return switch (kind) {
        .inference => .inference,
        .web_search => .web_search,
        .external_io => .external_io,
        .cdc => .cdc,
    };
}

fn webSearchConfigured(provider: []const u8, cfg: common_config.Config.WebSearchConnectionConfig) bool {
    if (std.mem.eql(u8, provider, "vertex")) {
        return cfg.data_store != null and (cfg.credentials_path != null or cfg.project_id != null);
    }
    return cfg.api_key != null;
}

fn isKnownWebSearchProvider(provider: []const u8) bool {
    inline for (&.{ "exa", "tavily", "brave", "serper", "you", "linkup", "vertex" }) |known| {
        if (std.mem.eql(u8, provider, known)) return true;
    }
    return false;
}

fn externalIoProtocolFromConfig(protocol: common_config.Config.ExternalIoProtocol) ExternalIoProtocol {
    return switch (protocol) {
        .s3 => .s3,
        .gcs => .gcs,
        .filesystem => .filesystem,
        .http => .http,
    };
}

fn configuredModelTypeSet(values: []const []const u8) !std.EnumSet(ConfiguredModelType) {
    var set = std.EnumSet(ConfiguredModelType).initEmpty();
    for (values) |value| {
        const model_type = std.meta.stringToEnum(ConfiguredModelType, value) orelse return error.InvalidConfig;
        set.insert(model_type);
    }
    return set;
}

fn instanceFromConnectionConfig(
    arena: Allocator,
    id: []const u8,
    provider: list_models.ProviderTag,
    cfg: common_config.Config.InferenceConnectionConfig,
    model_types: std.EnumSet(ConfiguredModelType),
) !*Instance {
    const instance = try arena.create(Instance);
    instance.* = .{
        .provider = provider,
        .url = if (cfg.url) |value| try arena.dupe(u8, value) else "",
        .api_key = if (cfg.api_key) |value| try arena.dupe(u8, value) else null,
        .region = if (cfg.region) |value| try arena.dupe(u8, value) else "",
        .project_id = if (cfg.project_id) |value| try arena.dupe(u8, value) else "",
        .location = if (cfg.location) |value| try arena.dupe(u8, value) else "",
        .credentials_path = if (cfg.credentials_path) |value| try arena.dupe(u8, value) else "",
        .key = try std.fmt.allocPrint(arena, "config:connections/{s}", .{id}),
        .model_types = model_types,
    };
    for (cfg.names) |name| try instance.names.append(arena, name);
    try instance.sources.append(arena, instance.key);
    return instance;
}

fn sourcesSlice(arena: Allocator, comptime fmt: []const u8, name: []const u8) ![]const []const u8 {
    const out = try arena.alloc([]const u8, 1);
    out[0] = try std.fmt.allocPrint(arena, fmt, .{name});
    return out;
}

fn cdcStatusFromReplicationStatus(status: ?*const table_manager.ReplicationSourceStatusRecord) ConnectionStatus {
    const record = status orelse return .configured;
    if (record.last_error.len > 0 or record.failure_class.len > 0 or std.mem.eql(u8, record.phase, "failed")) {
        return .@"error";
    }
    if (record.phase.len > 0 and !std.mem.eql(u8, record.phase, "configured")) return .connected;
    return .configured;
}

fn findTableByName(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?*const table_manager.TableRecord {
    for (snapshot.tables) |*table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn findReplicationSourceStatus(
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
    source_ordinal: u32,
) ?*const table_manager.ReplicationSourceStatusRecord {
    for (snapshot.replication_source_statuses) |*status| {
        if (status.table_id == table_id and status.source_ordinal == source_ordinal) return status;
    }
    return null;
}

fn nonEmpty(value: []const u8) ?[]const u8 {
    return if (value.len > 0) value else null;
}

fn nonZero(value: u64) ?u64 {
    return if (value == 0) null else value;
}

/// Group listed models by task type into the wire map, marking models that a
/// config references as configured.
fn modelsMapAlloc(arena: Allocator, models: []const list_models.ListedModel, instance: *const Instance) !std.json.ArrayHashMap([]const ConnectedModel) {
    var groups = std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(ConnectedModel)){};
    for (models) |model| {
        const group_key = model.kind.groupKey();
        const gop = try groups.getOrPut(arena, group_key);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        try gop.value_ptr.append(arena, .{
            .name = try arena.dupe(u8, model.name),
            .display_name = if (model.display_name) |value| try arena.dupe(u8, value) else null,
            .dimensions = model.dimensions,
            .configured = if (instance.configured_models.contains(model.name)) true else null,
        });
    }
    var map = std.json.ArrayHashMap([]const ConnectedModel){};
    var it = groups.iterator();
    while (it.next()) |entry| {
        try map.map.put(arena, entry.key_ptr.*, entry.value_ptr.items);
    }
    return map;
}

/// Resolve model listings per instance, serving from the cache when fresh and
/// using bounded shared-runtime fanout for live calls.
fn resolveModels(
    arena: Allocator,
    sources: Sources,
    cache: ?*Cache,
    opts: BuildOptions,
    instances: []const *Instance,
) ![]?ModelsOutcome {
    const outcomes = try arena.alloc(?ModelsOutcome, instances.len);
    @memset(outcomes, null);

    const now_ns = platform_time.monotonicNs();

    // Pending live fetches after cache lookups and local fast paths.
    var pending = std.ArrayListUnmanaged(struct { index: usize, job: *ModelsJob }).empty;

    for (instances, 0..) |instance, i| {
        if (instance.provider == .mock) {
            const result = try list_models.listModels(arena, undefined, .{ .provider = .mock }, opts.timeout_ms);
            outcomes[i] = .{ .ok = true, .models = result.models };
            continue;
        }

        // Embedded inference node: call in-process when available and the
        // instance has no remote URL override.
        if (instance.provider == .antfly and instance.url.len == 0 or
            (instance.provider == .antfly and sources.inference_api_url != null and std.mem.eql(u8, instance.url, sources.inference_api_url.?)))
        {
            if (sources.antfly_provider) |provider| {
                if (provider.list_models_json) |list_fn| {
                    if (list_fn(provider.ptr, arena)) |body| {
                        const result = try list_models.parseAntflyModels(arena, body);
                        outcomes[i] = .{ .ok = true, .models = result.models };
                    } else |err| {
                        outcomes[i] = .{ .ok = false, .err_name = @errorName(err) };
                    }
                    continue;
                }
            }
        }

        if (!opts.refresh) {
            if (cache) |c| {
                if (try c.lookupCopy(arena, instance.key, now_ns, opts.ttl_ns)) |entry| {
                    outcomes[i] = .{
                        .ok = entry.ok,
                        .err_name = entry.err_name,
                        .models = entry.models,
                    };
                    continue;
                }
            }
        }

        const api_key = resolveInferenceApiKey(arena, sources.secret_store, instance.api_key) catch |err| {
            outcomes[i] = .{ .ok = false, .err_name = @errorName(err) };
            continue;
        };

        const job = try arena.create(ModelsJob);
        job.* = .{
            .ep = .{
                .provider = instance.provider,
                .url = instance.url,
                .api_key = api_key,
                .region = instance.region,
                .project_id = instance.project_id,
                .location = instance.location,
                .credentials_path = instance.credentials_path,
            },
            .timeout_ms = opts.timeout_ms,
            .arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .cache = cache,
            .cache_key = instance.key,
            .request_started_ns = now_ns,
            .ttl_ns = opts.ttl_ns,
            .refresh = opts.refresh,
            .io = sources.io,
        };
        try pending.append(arena, .{ .index = i, .job = job });
    }

    // Run pending jobs in capped batches on the server-owned std.Io runtime.
    // This bounds process resources across requests instead of creating an OS
    // thread per connection. Embedded/test callers without a runtime execute
    // sequentially and retain identical semantics.
    const max_workers = @min(@max(opts.max_workers, 1), 64);
    var offset: usize = 0;
    while (offset < pending.items.len) {
        const end = @min(pending.items.len, offset + max_workers);
        if (sources.io) |io| {
            var group: std.Io.Group = .init;
            for (pending.items[offset..end]) |item| {
                group.concurrent(io, ModelsJob.run, .{item.job}) catch |err| {
                    item.job.err = err;
                };
            }
            group.await(io) catch {};
        } else {
            for (pending.items[offset..end]) |item| item.job.run() catch |err| {
                item.job.err = err;
            };
        }
        offset = end;
    }

    for (pending.items) |item| {
        const instance = instances[item.index];
        var outcome: ModelsOutcome = undefined;
        if (item.job.result) |result| {
            // Copy out of the job arena into the response arena.
            const models = try arena.alloc(list_models.ListedModel, result.models.len);
            for (result.models, 0..) |model, i| {
                models[i] = .{
                    .name = try arena.dupe(u8, model.name),
                    .display_name = if (model.display_name) |value| try arena.dupe(u8, value) else null,
                    .kind = model.kind,
                    .dimensions = model.dimensions,
                };
            }
            outcome = .{ .ok = true, .models = models };
        } else {
            outcome = .{ .ok = false, .err_name = item.job.cached_err_name orelse @errorName(item.job.err orelse error.Unknown) };
        }
        item.job.arena_state.deinit();
        outcomes[item.index] = outcome;

        if (cache) |c| {
            c.store(instance.key, .{
                .captured_at_ns = now_ns,
                .ok = outcome.ok,
                .err_name = @constCast(outcome.err_name),
                .models = @constCast(outcome.models),
            }) catch |err| {
                std.log.warn("connections: cache store failed err={}", .{err});
            };
        }
    }

    return outcomes;
}

const ProbeResult = struct {
    status: ConnectionStatus,
    err_name: ?[]const u8 = null,
};

const ObjectProbeJob = struct {
    cfg: common_config.Config.ExternalIoConnectionConfig,
    timeout_ms: u64,
    arena_state: std.heap.ArenaAllocator,
    failed_bucket: ?usize = null,
    err: ?anyerror = null,
    cache: ?*Cache = null,
    cache_key: []const u8 = "",
    request_started_ns: u64 = 0,
    ttl_ns: u64 = 0,
    refresh: bool = false,
    cached_err_name: ?[]const u8 = null,
    io: ?std.Io = null,

    fn run(job: *ObjectProbeJob) std.Io.Cancelable!void {
        const key_lock = if (job.io) |io|
            if (job.cache) |cache| .{ .lock = cache.keyLock(job.cache_key), .io = io } else null
        else
            null;
        if (key_lock) |guard| guard.lock.lockUncancelable(guard.io);
        defer if (key_lock) |guard| guard.lock.unlock(guard.io);
        if (job.cache) |cache| {
            const now = platform_time.monotonicNs();
            if (cache.lookupCopy(job.arena_state.allocator(), job.cache_key, now, job.ttl_ns) catch null) |entry| {
                if (!job.refresh or entry.captured_at_ns >= job.request_started_ns) {
                    if (!entry.ok) {
                        job.err = error.CachedProbeFailure;
                        job.cached_err_name = entry.err_name;
                    }
                    return;
                }
            }
        }
        probeObjectBuckets(job.arena_state.allocator(), job.cfg, job.timeout_ms, &job.failed_bucket, job.io) catch |err| {
            job.err = err;
            if (job.cache) |cache| cache.store(job.cache_key, .{
                .captured_at_ns = platform_time.monotonicNs(),
                .ok = false,
                .err_name = @constCast(@errorName(err)),
            }) catch {};
            return;
        };
        if (job.cache) |cache| cache.store(job.cache_key, .{
            .captured_at_ns = platform_time.monotonicNs(),
            .ok = true,
        }) catch {};
    }
};

/// Probes configured object-store connections concurrently through a bounded worker
/// pool. One worker resolves credentials once and reuses one HTTP client for
/// every bucket belonging to that connection.
fn resolveExternalIoProbes(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    node_config: *const common_config.Config,
    cache: ?*Cache,
    opts: BuildOptions,
    io: ?std.Io,
    secret_store: ?*common_secrets.FileStore,
) !void {
    const Pending = struct {
        connection_index: usize,
        cache_key: []const u8,
        job: *ObjectProbeJob,
    };
    var pending = std.ArrayListUnmanaged(Pending).empty;
    defer for (pending.items) |item| item.job.arena_state.deinit();
    const now_ns = platform_time.monotonicNs();

    for (connections.items, 0..) |*connection, connection_index| {
        if (connection.kind != .external_io) continue;
        const protocol = connection.external_io.?.protocol;
        if (protocol == .filesystem) {
            const configured = node_config.connections.get(connection.id) orelse return error.InvalidConfig;
            const cfg = configured.external_io orelse return error.InvalidConfig;
            probeFilesystemRoot(cfg.root orelse "", io) catch |err| {
                connection.status = .@"error";
                connection.@"error" = @errorName(err);
                continue;
            };
            connection.status = .connected;
            continue;
        }
        if (protocol != .s3 and protocol != .gcs) {
            connection.status = .unsupported;
            continue;
        }
        const configured = node_config.connections.get(connection.id) orelse return error.InvalidConfig;
        const raw_cfg = configured.external_io orelse return error.InvalidConfig;
        var resolved_credentials = common_config.Config.resolveExternalIoCredentials(arena, raw_cfg, secret_store) catch |err| {
            connection.status = .@"error";
            connection.@"error" = @errorName(err);
            continue;
        };
        const cfg = resolved_credentials.apply(raw_cfg);
        if (cfg.buckets.len == 0) {
            connection.status = .@"error";
            connection.@"error" = "MissingBucketAllowlist";
            continue;
        }

        const cache_key = try objectProbeCacheKeyAlloc(arena, connection.id, cfg);
        if (!opts.refresh) {
            if (cache) |c| {
                if (try c.lookupCopy(arena, cache_key, now_ns, opts.ttl_ns)) |entry| {
                    connection.status = if (entry.ok) .connected else .@"error";
                    connection.@"error" = if (entry.ok) null else entry.err_name;
                    continue;
                }
            }
        }

        const job = try arena.create(ObjectProbeJob);
        job.* = .{
            .cfg = cfg,
            .timeout_ms = @max(opts.timeout_ms, 1),
            .arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator),
            .cache = cache,
            .cache_key = cache_key,
            .request_started_ns = now_ns,
            .ttl_ns = opts.ttl_ns,
            .refresh = opts.refresh,
            .io = io,
        };
        try pending.append(arena, .{ .connection_index = connection_index, .cache_key = cache_key, .job = job });
    }

    const max_workers = @min(@max(opts.max_workers, 1), 64);
    var offset: usize = 0;
    while (offset < pending.items.len) {
        const end = @min(pending.items.len, offset + max_workers);
        if (io) |runtime_io| {
            var group: std.Io.Group = .init;
            for (pending.items[offset..end]) |item| {
                group.concurrent(runtime_io, ObjectProbeJob.run, .{item.job}) catch |err| {
                    item.job.err = err;
                };
            }
            group.await(runtime_io) catch {};
        } else {
            for (pending.items[offset..end]) |item| item.job.run() catch |err| {
                item.job.err = err;
            };
        }
        offset = end;
    }

    for (pending.items) |item| {
        const connection = &connections.items[item.connection_index];
        const outcome: ProbeResult = if (item.job.err) |err| .{
            .status = .@"error",
            .err_name = if (item.job.failed_bucket) |bucket_index|
                try std.fmt.allocPrint(arena, "bucket {s}: {s}", .{ item.job.cfg.buckets[bucket_index], @errorName(err) })
            else
                item.job.cached_err_name orelse @errorName(err),
        } else .{ .status = .connected };
        connection.status = outcome.status;
        connection.@"error" = outcome.err_name;

        if (cache) |c| {
            c.store(item.cache_key, .{
                // TTL starts when the probe completes, not before potentially
                // slow credential resolution and network I/O.
                .captured_at_ns = platform_time.monotonicNs(),
                .ok = outcome.status == .connected,
                .err_name = @constCast(outcome.err_name orelse ""),
            }) catch |err| {
                std.log.warn("connections: probe cache store failed err={}", .{err});
            };
        }
    }
}

fn objectProbeCacheKeyAlloc(arena: Allocator, name: []const u8, cfg: common_config.Config.ExternalIoConnectionConfig) ![]u8 {
    // Credential material participates in identity so rotation invalidates a
    // stale probe, but only its cryptographic digest is retained as a cache
    // key. A full digest also makes cross-connection aliasing negligible.
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hashProbeField(&hash, name);
    hashProbeField(&hash, @tagName(cfg.protocol));
    hashProbeOptionalField(&hash, cfg.endpoint);
    hashProbeOptionalField(&hash, cfg.region);
    hashProbeField(&hash, @tagName(cfg.addressing_style));
    hashProbeField(&hash, if (cfg.use_ssl orelse true) "tls" else "plain");
    for (cfg.buckets) |bucket| hashProbeField(&hash, bucket);
    hashProbeField(&hash, @tagName(cfg.credentials.source));
    hashProbeOptionalField(&hash, cfg.credentials.access_key_id);
    hashProbeOptionalField(&hash, cfg.credentials.secret_access_key);
    hashProbeOptionalField(&hash, cfg.credentials.session_token);
    hashProbeOptionalField(&hash, cfg.credentials.profile);
    hashProbeOptionalField(&hash, cfg.credentials.shared_credentials_file);
    hashProbeOptionalField(&hash, cfg.credentials.role_arn);
    hashProbeOptionalField(&hash, cfg.credentials.token_file);
    hashProbeOptionalField(&hash, cfg.credentials.session_name);
    hashProbeOptionalField(&hash, cfg.credentials.sts_endpoint);
    hashProbeOptionalField(&hash, cfg.project_id);
    hashProbeOptionalField(&hash, cfg.upload_endpoint);
    hashProbeField(&hash, @tagName(cfg.gcs_credentials.source));
    hashProbeOptionalField(&hash, cfg.gcs_credentials.bearer_token);
    hashProbeOptionalField(&hash, cfg.gcs_credentials.service_account_json);
    hashProbeOptionalField(&hash, cfg.gcs_credentials.credentials_path);
    hashProbeOptionalField(&hash, cfg.gcs_credentials.scope);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hash.final(&digest);
    const digest_hex = std.fmt.bytesToHex(digest, .lower);
    return try std.fmt.allocPrint(arena, "objectstore\x1f{s}", .{digest_hex});
}

fn hashProbeField(hash: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    const len: u64 = @intCast(value.len);
    hash.update(std.mem.asBytes(&len));
    hash.update(value);
}

fn hashProbeOptionalField(hash: *std.crypto.hash.sha2.Sha256, value: ?[]const u8) void {
    if (value) |present| {
        hash.update(&.{1});
        hashProbeField(hash, present);
    } else {
        hash.update(&.{0});
    }
}

fn probeS3Buckets(
    arena: Allocator,
    cfg: common_config.Config.ExternalIoConnectionConfig,
    timeout_ms: u64,
    failed_bucket: *?usize,
    shared_io: ?std.Io,
) !void {
    var dynamic_credentials: ?bedrock.Credentials = null;
    defer if (dynamic_credentials) |*credentials| credentials.deinit(arena);

    if (cfg.credentials.source != .static) {
        var io_impl: ?std.Io.Threaded = if (shared_io == null) std.Io.Threaded.init(arena, .{}) else null;
        defer if (io_impl) |*owned| owned.deinit();
        var http = httpx.Client.initWithConfig(arena, shared_io orelse io_impl.?.io(), .{ .timeouts = .{
            .connect_ms = timeout_ms,
            .read_ms = timeout_ms,
            .write_ms = timeout_ms,
            .request_ms = timeout_ms,
        } });
        defer http.deinit();
        var credential_cache: bedrock.CredentialCache = .{};
        defer credential_cache.deinit(arena);
        const source: bedrock.CredentialSource = switch (cfg.credentials.source) {
            .default => .default,
            .static => unreachable,
            .profile => .{ .profile = .{
                .name = cfg.credentials.profile orelse return error.InvalidConnectionCredentials,
                .shared_credentials_file = cfg.credentials.shared_credentials_file,
            } },
            .web_identity => .{ .web_identity = .{
                .role_arn = cfg.credentials.role_arn orelse return error.InvalidConnectionCredentials,
                .token_file = cfg.credentials.token_file orelse return error.InvalidConnectionCredentials,
                .session_name = cfg.credentials.session_name orelse "antfly-connection-probe",
                .sts_endpoint = cfg.credentials.sts_endpoint,
            } },
        };
        dynamic_credentials = try credential_cache.getForSource(arena, &http, cfg.region orelse "us-east-1", source);
    }

    const access_key_id = if (dynamic_credentials) |credentials| credentials.access_key_id else cfg.credentials.access_key_id orelse return error.InvalidConnectionCredentials;
    const secret_access_key = if (dynamic_credentials) |credentials| credentials.secret_access_key else cfg.credentials.secret_access_key orelse return error.InvalidConnectionCredentials;
    const session_token = if (dynamic_credentials) |credentials| credentials.session_token else cfg.credentials.session_token;
    var s3_config = try objectstore.s3.fromEnvAlloc(
        arena,
        cfg.endpoint,
        cfg.use_ssl orelse true,
        access_key_id,
        secret_access_key,
        session_token,
        cfg.region,
        switch (cfg.addressing_style) {
            .path => .path,
            .virtual_hosted => .virtual_hosted,
        },
    );
    s3_config.request_timeout_ms = timeout_ms;
    s3_config.io = shared_io;
    var s3_client = try objectstore.s3.Client.init(arena, s3_config);
    var client = s3_client.client();
    defer client.deinit();
    for (cfg.buckets, 0..) |bucket, bucket_index| {
        const exists = client.bucketExists(bucket) catch |err| {
            failed_bucket.* = bucket_index;
            return err;
        };
        if (!exists) {
            failed_bucket.* = bucket_index;
            return error.BucketNotFound;
        }
    }
}

fn probeObjectBuckets(
    arena: Allocator,
    cfg: common_config.Config.ExternalIoConnectionConfig,
    timeout_ms: u64,
    failed_bucket: *?usize,
    io: ?std.Io,
) !void {
    return switch (cfg.protocol) {
        .s3 => probeS3Buckets(arena, cfg, timeout_ms, failed_bucket, io),
        .gcs => probeGcsBuckets(arena, cfg, timeout_ms, failed_bucket, io),
        else => error.InvalidConfig,
    };
}

fn probeGcsBuckets(
    arena: Allocator,
    cfg: common_config.Config.ExternalIoConnectionConfig,
    timeout_ms: u64,
    failed_bucket: *?usize,
    io: ?std.Io,
) !void {
    var gcs_cfg = try backups_api.gcsConfigForConnection(arena, cfg, io);
    gcs_cfg.request_timeout_ms = timeout_ms;
    var gcs = try objectstore.Gcs.JsonApiClient.init(arena, gcs_cfg);
    var client = gcs.client();
    defer client.deinit();
    for (cfg.buckets, 0..) |bucket, bucket_index| {
        const exists = client.bucketExists(bucket) catch |err| {
            failed_bucket.* = bucket_index;
            return err;
        };
        if (!exists) {
            failed_bucket.* = bucket_index;
            return error.BucketNotFound;
        }
    }
}

fn probeFilesystemRoot(root: []const u8, shared_io: ?std.Io) !void {
    if (!std.fs.path.isAbsolute(root)) return error.InvalidFilesystemRoot;
    var io_impl: ?std.Io.Threaded = if (shared_io == null) std.Io.Threaded.init(std.heap.page_allocator, .{}) else null;
    defer if (io_impl) |*owned| owned.deinit();
    const io = shared_io orelse io_impl.?.io();
    var dir = try std.Io.Dir.openDirAbsolute(io, root, .{});
    defer dir.close(io);
}

// --- Tests ---

const table_manager = @import("../metadata/table_manager.zig");

test "object probe cache identity covers every bucket and credential source" {
    const alloc = std.testing.allocator;
    var first = common_config.Config.ExternalIoConnectionConfig{
        .protocol = .s3,
        .buckets = @constCast(&[_][]u8{ @constCast("bucket-one"), @constCast("bucket-two") }),
        .credentials = .{ .source = .profile, .profile = @constCast("reader-a") },
    };
    const first_key = try objectProbeCacheKeyAlloc(alloc, "archive", first);
    defer alloc.free(first_key);
    first.buckets = @constCast(&[_][]u8{ @constCast("bucket-one"), @constCast("bucket-three") });
    const bucket_key = try objectProbeCacheKeyAlloc(alloc, "archive", first);
    defer alloc.free(bucket_key);
    try std.testing.expect(!std.mem.eql(u8, first_key, bucket_key));
    first.credentials.profile = @constCast("reader-b");
    const credential_key = try objectProbeCacheKeyAlloc(alloc, "archive", first);
    defer alloc.free(credential_key);
    try std.testing.expect(!std.mem.eql(u8, bucket_key, credential_key));
}

test "connection cache remains valid across every allocation failure" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var cache = Cache.init(alloc);
            defer cache.deinit();
            try cache.store("provider", .{
                .captured_at_ns = 1,
                .ok = false,
                .err_name = @constCast("first failure"),
            });
            try cache.store("provider", .{
                .captured_at_ns = 2,
                .ok = true,
            });
            const entry = (try cache.lookupCopy(alloc, "provider", 2, 1)).?;
            var owned = entry;
            defer owned.deinitOwned(alloc);
            try std.testing.expect(owned.ok);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "build response exposes embedded inference as a local connection" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();

    const provider = managed_embedder.AntflyProvider{
        .ptr = undefined,
        .embed_dense_texts = undefined,
        .embed_sparse_texts = undefined,
    };
    const response = try buildConnectionsResponse(
        arena_state.allocator(),
        .{ .antfly_provider = provider },
        null,
        .{ .types_filter = "inference" },
    );

    try std.testing.expectEqual(@as(usize, 1), response.connections.len);
    try std.testing.expectEqualStrings("local-inference", response.connections[0].id);
    try std.testing.expectEqual(ConnectionKind.inference, response.connections[0].kind);
    try std.testing.expectEqual(list_models.ProviderTag.antfly, response.connections[0].inference.?.provider);
}

test "inference connection operations are allowlisted" {
    try std.testing.expectEqualStrings("models.generate", inferenceCapability("generate").?);
    try std.testing.expectEqualStrings("models.embed", inferenceCapability("embed").?);
    try std.testing.expect(inferenceCapability("../models") == null);

    const embed_only = &.{"models.embed"};
    try requireInferenceCapability(embed_only, inferenceCapability("embed").?);
    try std.testing.expectError(error.ConnectionCapabilityMissing, requireInferenceCapability(embed_only, inferenceCapability("generate").?));

    const alloc = std.testing.allocator;
    var cfg = try common_config.Config.parseFromSlice(alloc,
        \\{
        \\  "connections": {
        \\    "embed-only": {
        \\      "kind": "inference",
        \\      "capabilities": ["models.embed"],
        \\      "inference": { "provider": "antfly", "url": "https://platform.antfly.io" }
        \\    }
        \\  }
        \\}
    );
    defer cfg.deinit();
    try std.testing.expectError(error.ConnectionCapabilityMissing, invokeInferenceConnection(alloc, undefined, &cfg, null, null, null, "embed-only", "generate", "{}"));
    try std.testing.expectEqual(
        InferenceAdmissionOwner.caller,
        try inferenceConnectionAdmissionOwner(&cfg, "http://127.0.0.1:8080", "embed-only", "embed"),
    );
    try std.testing.expectEqual(
        InferenceAdmissionOwner.target,
        try inferenceConnectionAdmissionOwner(null, "http://127.0.0.1:8080", common_config.local_inference_connection_id, "generate"),
    );
    try std.testing.expect(sameInferenceBaseURL("http://127.0.0.1:8080/ai/v1/", "HTTP://127.0.0.1:8080"));

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-connection-secrets-{d}.json", .{platform_time.monotonicNs()});
    defer alloc.free(store_path);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    defer std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};

    var secret_store = try common_secrets.FileStore.init(alloc, store_path);
    defer secret_store.deinit();
    var stored = try secret_store.put(alloc, "openai.api_key", "resolved-key");
    defer stored.deinit(alloc);

    const resolved = (try resolveInferenceApiKey(alloc, &secret_store, "${secret:openai.api_key}")).?;
    defer alloc.free(resolved);
    try std.testing.expectEqualStrings("resolved-key", resolved);
}

test "inference connection URLs require an HTTP origin" {
    try std.testing.expect(validInferenceURL("https://platform.antfly.io/ai/v1"));
    try std.testing.expect(validInferenceURL("http://antfly-inference.default.svc:8080"));
    try std.testing.expect(!validInferenceURL("file:///etc/passwd"));
    try std.testing.expect(!validInferenceURL("https://platform.antfly.io/ai/v1?redirect=bad"));
}

test "build response reports mock connected and types filter" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "connections": {
        \\    "mocked": {
        \\      "kind": "inference",
        \\      "capabilities": ["models.generate", "models.embed"],
        \\      "inference": {
        \\        "provider": "mock",
        \\        "names": ["mocked"],
        \\        "configured_model_types": ["generator", "embedder"]
        \\      }
        \\    }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try common_config.Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    const response = try buildConnectionsResponse(arena, .{ .node_config = &cfg }, null, .{ .include_models = true });
    try std.testing.expectEqual(@as(usize, 1), response.connections.len);
    const connection = response.connections[0];
    try std.testing.expectEqualStrings("mocked", connection.name);
    try std.testing.expectEqual(ConnectionKind.inference, connection.kind);
    try std.testing.expectEqual(ConnectionStatus.connected, connection.status);
    try std.testing.expect(containsString(connection.capabilities, "models.generate"));
    try std.testing.expect(containsString(connection.inference.?.configured_model_types, "generator"));
    const models = connection.inference.?.models.?;
    try std.testing.expect(models.map.get("embedders") != null);
    try std.testing.expect(models.map.get("generators") != null);

    const filtered = try buildConnectionsResponse(arena, .{ .node_config = &cfg }, null, .{ .types_filter = "external_io" });
    try std.testing.expectEqual(@as(usize, 0), filtered.connections.len);

    const invalid = try buildConnectionsResponse(arena, .{ .node_config = &cfg }, null, .{ .types_filter = "object_store" });
    try std.testing.expectEqual(@as(usize, 0), invalid.connections.len);
}

test "build response reports configured external io connections" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "connections": {
        \\    "backups": {
        \\      "kind": "external_io",
        \\      "capabilities": ["objects.read", "objects.write", "backup.write", "restore.read"],
        \\      "external_io": {
        \\        "protocol": "s3",
        \\        "endpoint": "s3.amazonaws.com",
        \\        "buckets": ["antfly-prod"],
        \\        "prefix": "cluster-a/"
        \\      }
        \\    },
        \\    "docs-site": {
        \\      "kind": "external_io",
        \\      "capabilities": ["content.fetch", "indexing.use", "agents.use"],
        \\      "external_io": {
        \\        "protocol": "http",
        \\        "hosts": ["https://docs.example.com"]
        \\      }
        \\    }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try common_config.Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    const response = try buildConnectionsResponse(
        arena,
        .{ .node_config = &cfg },
        null,
        .{ .types_filter = "external_io" },
    );
    try std.testing.expectEqual(@as(usize, 2), response.connections.len);
    for (response.connections) |connection| {
        try std.testing.expectEqual(ConnectionKind.external_io, connection.kind);
        try std.testing.expect(connection.id.len > 0);
        try std.testing.expect(connection.external_io != null);
        try std.testing.expectEqual(ConnectionStatus.configured, connection.status);
    }

    const backups = response.connections[0];
    try std.testing.expectEqualStrings("backups", backups.id);
    try std.testing.expectEqual(ExternalIoProtocol.s3, backups.external_io.?.protocol);
    try std.testing.expect(containsString(backups.capabilities, "objects.read"));
    try std.testing.expect(containsString(backups.capabilities, "backup.write"));
    try std.testing.expectEqualStrings("antfly-prod", backups.external_io.?.buckets[0]);

    const docs_http = response.connections[1];
    try std.testing.expectEqualStrings("docs-site", docs_http.id);
    try std.testing.expectEqual(ExternalIoProtocol.http, docs_http.external_io.?.protocol);
    try std.testing.expect(containsString(docs_http.capabilities, "content.fetch"));
    try std.testing.expectEqualStrings("https://docs.example.com", docs_http.external_io.?.hosts[0]);

    const invalid_filter = try buildConnectionsResponse(
        arena,
        .{ .node_config = &cfg },
        null,
        .{ .types_filter = "object_store", .probe = false },
    );
    try std.testing.expectEqual(@as(usize, 0), invalid_filter.connections.len);
}

test "build response reports configured web search connections" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "connections": {
        \\    "agent-web": {
        \\      "display_name": "Agent web",
        \\      "kind": "web_search",
        \\      "provider": "exa",
        \\      "capabilities": ["web.search", "web.semantic_search", "web.fetch", "agents.use"],
        \\      "web_search": {
        \\        "max_results": 10,
        \\        "timeout_ms": 12000,
        \\        "safe_search": true,
        \\        "language": "en",
        \\        "region": "us",
        \\        "include_content": true,
        \\        "include_highlights": true,
        \\        "api_key": "test-only-key",
        \\        "include_domains": ["docs.example.com"]
        \\      }
        \\    }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try common_config.Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    const response = try buildConnectionsResponse(
        arena,
        .{ .node_config = &cfg },
        null,
        .{ .types_filter = "web_search" },
    );
    try std.testing.expectEqual(@as(usize, 1), response.connections.len);
    const connection = response.connections[0];
    try std.testing.expectEqualStrings("agent-web", connection.id);
    try std.testing.expectEqualStrings("Agent web", connection.display_name.?);
    try std.testing.expectEqual(ConnectionKind.web_search, connection.kind);
    try std.testing.expectEqualStrings("exa", connection.provider.?);
    try std.testing.expectEqual(ConnectionStatus.configured, connection.status);
    try std.testing.expect(containsString(connection.capabilities, "web.search"));
    try std.testing.expect(containsString(connection.capabilities, "agents.use"));
    try std.testing.expectEqual(@as(?u32, 10), connection.web_search.?.max_results);
    try std.testing.expectEqual(@as(?u32, 12000), connection.web_search.?.timeout_ms);
    try std.testing.expectEqual(true, connection.web_search.?.safe_search.?);
    try std.testing.expectEqualStrings("en", connection.web_search.?.language.?);
    try std.testing.expectEqualStrings("us", connection.web_search.?.region.?);
    try std.testing.expectEqual(true, connection.web_search.?.include_content.?);
    try std.testing.expectEqual(true, connection.web_search.?.include_highlights.?);
    try std.testing.expectEqualStrings("docs.example.com", connection.web_search.?.include_domains[0]);
    try std.testing.expectEqual(true, connection.web_search.?.configured.?);
}

test "build response includes cdc replication sources with generic cdc kind" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const raw =
        \\{
        \\  "metadata": {
        \\    "orchestration_urls": {
        \\      "1": "http://127.0.0.1:7001"
        \\    }
        \\  },
        \\  "connections": {
        \\    "users-pg": {
        \\      "kind": "cdc",
        \\      "capabilities": ["cdc.read_stream"],
        \\      "cdc": {
        \\        "provider": "postgres",
        \\        "dsn": "postgres://example",
        \\        "table_name": "users",
        \\        "source_ordinal": 0,
        \\        "external_table": "public.users",
        \\        "slot_name": "slot_cfg",
        \\        "publication_name": "pub_cfg"
        \\      }
        \\    }
        \\  },
        \\  "replication_factor": 1,
        \\  "default_shards_per_table": 1,
        \\  "max_shard_size_bytes": 1024,
        \\  "max_shards_per_table": 4
        \\}
    ;
    var cfg = try common_config.Config.parseFromSlice(alloc, raw);
    defer cfg.deinit();

    var tables = [_]table_manager.TableRecord{.{
        .table_id = 7,
        .name = "users",
    }};
    var statuses = [_]table_manager.ReplicationSourceStatusRecord{.{
        .table_id = 7,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "public.users",
        .slot_name = "slot_live",
        .publication_name = "pub_live",
        .phase = "streaming",
        .lag_records = 2,
        .lag_millis = 34,
        .last_success_at_ms = 1000,
        .updated_at_ms = 1100,
    }};
    var snapshot = metadata_api.AdminSnapshot{
        .status = undefined,
        .tables = tables[0..],
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
        .replication_source_statuses = statuses[0..],
    };

    const response = try buildConnectionsResponse(arena, .{ .node_config = &cfg, .snapshot = &snapshot }, null, .{ .types_filter = "cdc" });
    try std.testing.expectEqual(@as(usize, 1), response.connections.len);
    const connection = response.connections[0];
    try std.testing.expectEqualStrings("users-pg", connection.id);
    try std.testing.expectEqual(ConnectionKind.cdc, connection.kind);
    try std.testing.expectEqual(ConnectionStatus.connected, connection.status);
    try std.testing.expect(containsString(connection.capabilities, "cdc.read_stream"));
    try std.testing.expectEqualStrings("postgres", connection.cdc.?.provider);
    try std.testing.expectEqualStrings("users", connection.cdc.?.table_name);
    try std.testing.expectEqualStrings("public.users", connection.cdc.?.external_table.?);
    try std.testing.expectEqualStrings("slot_live", connection.cdc.?.slot_name.?);
    try std.testing.expectEqual(@as(u64, 34), connection.cdc.?.lag_millis.?);
}

test "include param parsing" {
    try std.testing.expect(includeHasModels("models"));
    try std.testing.expect(includeHasModels("models, other"));
    try std.testing.expect(includeHasModels(" other ,models"));
    try std.testing.expect(!includeHasModels(null));
    try std.testing.expect(!includeHasModels(""));
    try std.testing.expect(!includeHasModels("modeling"));
    try std.testing.expect(includeHasStatus("status"));
    try std.testing.expect(includeHasStatus("models, status"));
    try std.testing.expect(!includeHasStatus(null));
    try std.testing.expect(!includeHasStatus("statuses"));
}
