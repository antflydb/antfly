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
// Enumerates inference provider instances (node-config provider registry plus
// per-table embedding index configs, deduped by provider identity), external
// IO endpoints, and CDC sources. With the "models"
// expansion each inference provider's list-models API is queried live;
// per-connection failures degrade to status "error" without failing the
// response.

const std = @import("std");
const builtin = @import("builtin");
const httpx = @import("httpx");
const embeddings = @import("antfly_embeddings");
const objectstore = @import("objectstore");
const platform_time = @import("../platform/time.zig");
const provider_registry = @import("../common/provider_registry.zig");
const common_config = @import("../common/config.zig");
const metadata_api = @import("../metadata/api.zig");
const list_models = @import("../inference/list_models.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");

const Allocator = std.mem.Allocator;

pub const ConnectionKind = enum {
    inference,
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
    kind: ConnectionKind,
    status: ConnectionStatus,
    @"error": ?[]const u8 = null,
    capabilities: []const []const u8 = &.{},
    sources: []const []const u8 = &.{},
    inference: ?InferenceConnection = null,
    external_io: ?ExternalIoConnection = null,
    cdc: ?CdcConnection = null,
};

pub const ConnectionsResponse = struct {
    connections: []const Connection = &.{},
};

pub const Sources = struct {
    registry: ?*const provider_registry.Registry = null,
    node_config: ?*const common_config.Config = null,
    snapshot: ?*const metadata_api.AdminSnapshot = null,
    antfly_provider: ?managed_embedder.AntflyProvider = null,
    inference_api_url: ?[]const u8 = null,
    inference_api_key: ?[]const u8 = null,
};

pub const BuildOptions = struct {
    include_models: bool = false,
    refresh: bool = false,
    probe: bool = true,
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
    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
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
        while (!self.mutex.tryLock()) {
            if (comptime builtin.os.tag == .freestanding) {
                std.atomic.spinLoopHint();
                continue;
            }
            std.Thread.yield() catch {};
        }
    }

    fn unlock(self: *Cache) void {
        self.mutex.unlock();
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
        const gop = try self.entries.getOrPut(self.alloc, key);
        if (gop.found_existing) {
            gop.value_ptr.deinitOwned(self.alloc);
        } else {
            gop.key_ptr.* = try self.alloc.dupe(u8, key);
        }
        gop.value_ptr.* = owned;
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

const Seed = struct {
    provider_name: []const u8,
    url: []const u8 = "",
    api_key: ?[]const u8 = null,
    region: []const u8 = "",
    project_id: []const u8 = "",
    location: []const u8 = "",
    credentials_path: []const u8 = "",
    model: []const u8 = "",
};

const GatherState = struct {
    arena: Allocator,
    instances: std.StringArrayHashMapUnmanaged(*Instance) = .{},

    fn addSeed(
        self: *GatherState,
        seed: Seed,
        name: ?[]const u8,
        source: []const u8,
        model_type: ?ConfiguredModelType,
    ) !void {
        const provider = std.meta.stringToEnum(list_models.ProviderTag, seed.provider_name) orelse return;
        const key = try instanceKeyAlloc(self.arena, provider, seed);
        const gop = try self.instances.getOrPut(self.arena, key);
        if (!gop.found_existing) {
            const instance = try self.arena.create(Instance);
            instance.* = .{
                .provider = provider,
                .url = try self.arena.dupe(u8, seed.url),
                .api_key = if (seed.api_key) |value| try self.arena.dupe(u8, value) else null,
                .region = try self.arena.dupe(u8, seed.region),
                .project_id = try self.arena.dupe(u8, seed.project_id),
                .location = try self.arena.dupe(u8, seed.location),
                .credentials_path = try self.arena.dupe(u8, seed.credentials_path),
                .key = key,
            };
            gop.value_ptr.* = instance;
        }
        const instance = gop.value_ptr.*;
        if (name) |value| {
            if (!containsString(instance.names.items, value)) {
                try instance.names.append(self.arena, try self.arena.dupe(u8, value));
            }
        }
        try instance.sources.append(self.arena, try self.arena.dupe(u8, source));
        if (model_type) |value| instance.model_types.insert(value);
        if (seed.model.len > 0) {
            const model_gop = try instance.configured_models.getOrPut(self.arena, seed.model);
            if (!model_gop.found_existing) model_gop.key_ptr.* = try self.arena.dupe(u8, seed.model);
        }
    }
};

fn containsString(haystack: []const []const u8, needle: []const u8) bool {
    for (haystack) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

fn instanceKeyAlloc(arena: Allocator, provider: list_models.ProviderTag, seed: Seed) ![]u8 {
    const key_hash: u64 = if (seed.api_key) |value| std.hash.Wyhash.hash(0, value) else 0;
    return try std.fmt.allocPrint(arena, "{s}\x1f{s}\x1f{s}\x1f{s}\x1f{s}\x1f{s}\x1f{x}", .{
        @tagName(provider),
        seed.url,
        seed.region,
        seed.project_id,
        seed.location,
        seed.credentials_path,
        key_hash,
    });
}

fn seedFromEmbedderConfig(cfg: embeddings.Config) Seed {
    return .{
        .provider_name = @tagName(cfg.provider),
        .url = cfg.url,
        .api_key = cfg.api_key,
        .region = cfg.region,
        .project_id = cfg.project_id,
        .location = cfg.location,
        .credentials_path = cfg.credentials_path,
        .model = cfg.model,
    };
}

fn gatherInstances(arena: Allocator, sources: Sources) !GatherState {
    var state = GatherState{ .arena = arena };

    if (sources.registry) |registry| {
        var embedder_it = registry.embedder_configs.iterator();
        while (embedder_it.next()) |entry| {
            const source = try std.fmt.allocPrint(arena, "config:embedders/{s}", .{entry.key_ptr.*});
            try state.addSeed(seedFromEmbedderConfig(entry.value_ptr.*), entry.key_ptr.*, source, .embedder);
        }
        var generator_it = registry.generator_configs.iterator();
        while (generator_it.next()) |entry| {
            const cfg = entry.value_ptr.*;
            const source = try std.fmt.allocPrint(arena, "config:generators/{s}", .{entry.key_ptr.*});
            try state.addSeed(.{
                .provider_name = @tagName(cfg.provider),
                .url = cfg.url,
                .api_key = cfg.api_key,
                .project_id = cfg.project_id orelse "",
                .location = cfg.location orelse "",
                .credentials_path = cfg.credentials_path orelse "",
                .model = cfg.model,
            }, entry.key_ptr.*, source, .generator);
        }
        var reranker_it = registry.reranker_configs.iterator();
        while (reranker_it.next()) |entry| {
            const cfg = entry.value_ptr.*;
            const source = try std.fmt.allocPrint(arena, "config:rerankers/{s}", .{entry.key_ptr.*});
            try state.addSeed(.{
                .provider_name = @tagName(cfg.provider),
                .url = cfg.url,
                .api_key = cfg.api_key,
                .project_id = cfg.project_id,
                .credentials_path = cfg.credentials_path,
                .model = cfg.model,
            }, entry.key_ptr.*, source, .reranker);
        }
        var chunker_it = registry.chunker_configs.iterator();
        while (chunker_it.next()) |entry| {
            const cfg = entry.value_ptr.*;
            const source = try std.fmt.allocPrint(arena, "config:chunkers/{s}", .{entry.key_ptr.*});
            try state.addSeed(.{
                .provider_name = @tagName(cfg.provider),
                .url = cfg.api_url,
                .model = cfg.model,
            }, entry.key_ptr.*, source, .chunker);
        }
    }

    if (sources.snapshot) |snapshot| {
        for (snapshot.tables) |table| {
            addTableEmbedderSeeds(&state, table.name, table.indexes_json) catch |err| {
                std.log.warn("connections: skipping table index configs table={s} err={}", .{ table.name, err });
            };
        }
    }

    // The local/configured inference service is always a connection, even when
    // no named config references it.
    if (sources.antfly_provider != null or sources.inference_api_url != null) {
        try state.addSeed(.{
            .provider_name = "antfly",
            .url = sources.inference_api_url orelse "",
            .api_key = sources.inference_api_key,
        }, null, "config:inference", null);
    }

    return state;
}

fn addTableEmbedderSeeds(state: *GatherState, table_name: []const u8, indexes_json: []const u8) !void {
    if (indexes_json.len == 0) return;
    var parsed = try std.json.parseFromSlice(std.json.Value, state.arena, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const embedder_value = entry.value_ptr.object.get("embedder") orelse continue;
        var cfg = embeddings.parseConfigFromValue(state.arena, embedder_value) catch |err| {
            std.log.warn("connections: skipping embedder config table={s} index={s} err={}", .{ table_name, entry.key_ptr.*, err });
            continue;
        };
        defer cfg.deinit(state.arena);
        const source = try std.fmt.allocPrint(state.arena, "table:{s}/index:{s}", .{ table_name, entry.key_ptr.* });
        try state.addSeed(seedFromEmbedderConfig(cfg), null, source, .embedder);
    }
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

    fn run(job: *ModelsJob) void {
        const alloc = job.arena_state.allocator();
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
        defer client.deinit();
        job.result = list_models.listModels(alloc, &client, job.ep, job.timeout_ms) catch |err| {
            job.err = err;
            return;
        };
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

    var connections = std.ArrayListUnmanaged(Connection).empty;
    var used_names = std.StringArrayHashMapUnmanaged(void){};

    if (kinds.contains(.inference)) {
        var state = try gatherInstances(arena, sources);
        const instances = state.instances.values();

        var outcomes: ?[]?ModelsOutcome = null;
        if (opts.include_models) {
            outcomes = try resolveModels(arena, sources, cache, opts, instances);
        }

        for (instances, 0..) |instance, i| {
            const name = try uniqueName(arena, &used_names, primaryInstanceName(instance));
            var connection = Connection{
                .id = name,
                .name = name,
                .kind = .inference,
                .status = if (instance.provider == .mock) .connected else .configured,
                .capabilities = try inferenceCapabilities(arena, instance.model_types),
                .sources = instance.sources.items,
                .inference = .{
                    .provider = instance.provider,
                    .url = if (instance.url.len > 0) instance.url else null,
                    .region = if (instance.region.len > 0) instance.region else null,
                    .project_id = if (instance.project_id.len > 0) instance.project_id else null,
                    .location = if (instance.location.len > 0) instance.location else null,
                    .names = instance.names.items,
                    .configured_model_types = try configuredModelTypeNames(arena, instance.model_types),
                },
            };
            if (outcomes) |values| {
                if (values[i]) |outcome| {
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
    }

    if (sources.node_config) |node_config| {
        if (kinds.contains(.external_io)) {
            try appendExternalIoConnections(arena, &connections, &used_names, node_config, cache, opts);
        }
    }

    if (kinds.contains(.cdc)) {
        if (sources.snapshot) |snapshot| {
            try appendCdcConnections(arena, &connections, &used_names, snapshot);
        }
    }

    return .{ .connections = connections.items };
}

fn parseKindFilter(filter: []const u8) std.EnumSet(ConnectionKind) {
    var kinds = std.EnumSet(ConnectionKind).initEmpty();
    var it = std.mem.splitScalar(u8, filter, ',');
    while (it.next()) |raw| {
        const trimmed = std.mem.trim(u8, raw, " \t");
        if (std.meta.stringToEnum(ConnectionKind, trimmed)) |kind| {
            kinds.insert(kind);
        } else if (std.mem.eql(u8, trimmed, "object_store") or
            std.mem.eql(u8, trimmed, "remote_content") or
            std.mem.eql(u8, trimmed, "remote_content_http"))
        {
            kinds.insert(.external_io);
        } else if (std.mem.eql(u8, trimmed, "inference_provider")) {
            kinds.insert(.inference);
        }
    }
    if (kinds.count() == 0) return std.EnumSet(ConnectionKind).initFull();
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

fn primaryInstanceName(instance: *const Instance) []const u8 {
    if (instance.names.items.len > 0) return instance.names.items[0];
    return @tagName(instance.provider);
}

fn uniqueName(arena: Allocator, used: *std.StringArrayHashMapUnmanaged(void), base: []const u8) ![]const u8 {
    var candidate: []const u8 = base;
    var suffix: usize = 2;
    while (used.contains(candidate)) : (suffix += 1) {
        candidate = try std.fmt.allocPrint(arena, "{s}-{d}", .{ base, suffix });
    }
    try used.put(arena, candidate, {});
    return candidate;
}

fn configuredModelTypeNames(arena: Allocator, set: std.EnumSet(ConfiguredModelType)) ![]const []const u8 {
    var names = std.ArrayListUnmanaged([]const u8).empty;
    inline for (@typeInfo(ConfiguredModelType).@"enum".fields) |field| {
        if (set.contains(@field(ConfiguredModelType, field.name))) {
            try names.append(arena, field.name);
        }
    }
    return names.items;
}

fn inferenceCapabilities(arena: Allocator, set: std.EnumSet(ConfiguredModelType)) ![]const []const u8 {
    var capabilities = std.ArrayListUnmanaged([]const u8).empty;
    if (set.contains(.generator)) {
        try capabilities.append(arena, "models.generate");
        try capabilities.append(arena, "agents.use");
    }
    if (set.contains(.embedder)) {
        try capabilities.append(arena, "models.embed");
        try capabilities.append(arena, "indexing.use");
    }
    if (set.contains(.reranker)) try capabilities.append(arena, "models.rerank");
    if (set.contains(.chunker)) {
        try capabilities.append(arena, "models.chunk");
        try capabilities.append(arena, "indexing.use");
    }
    return dedupeStringsInOrder(arena, capabilities.items);
}

fn dedupeStringsInOrder(arena: Allocator, values: []const []const u8) ![]const []const u8 {
    var seen = std.StringArrayHashMapUnmanaged(void){};
    var out = std.ArrayListUnmanaged([]const u8).empty;
    for (values) |value| {
        if (seen.contains(value)) continue;
        try seen.put(arena, value, {});
        try out.append(arena, value);
    }
    return out.items;
}

fn sourcesSlice(arena: Allocator, comptime fmt: []const u8, name: []const u8) ![]const []const u8 {
    const out = try arena.alloc([]const u8, 1);
    out[0] = try std.fmt.allocPrint(arena, fmt, .{name});
    return out;
}

fn cdcSourcesSlice(arena: Allocator, table_name: []const u8, source_ordinal: u32) ![]const []const u8 {
    const out = try arena.alloc([]const u8, 1);
    out[0] = try std.fmt.allocPrint(arena, "table:{s}/replication_sources/{d}", .{ table_name, source_ordinal });
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

fn appendCdcConnections(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    used_names: *std.StringArrayHashMapUnmanaged(void),
    snapshot: *const metadata_api.AdminSnapshot,
) !void {
    for (snapshot.tables) |table| {
        if (table.replication_sources_json.len == 0) continue;
        var parsed = std.json.parseFromSlice(std.json.Value, arena, table.replication_sources_json, .{}) catch |err| {
            std.log.warn("connections: skipping cdc sources table={s} err={}", .{ table.name, err });
            continue;
        };
        defer parsed.deinit();
        if (parsed.value != .array) continue;

        for (parsed.value.array.items, 0..) |source, source_i| {
            if (source != .object) continue;
            const provider = valueString(source.object.get("type")) orelse continue;
            const source_ordinal: u32 = @intCast(source_i);
            const status = findReplicationSourceStatus(snapshot, table.table_id, source_ordinal);
            const external_table = if (status) |record|
                nonEmpty(record.external_table)
            else
                valueString(source.object.get("postgres_table"));
            const slot_name = if (status) |record|
                nonEmpty(record.slot_name)
            else
                valueString(source.object.get("slot_name"));
            const publication_name = if (status) |record|
                nonEmpty(record.publication_name)
            else
                valueString(source.object.get("publication_name"));
            const phase = if (status) |record| nonEmpty(record.phase) else null;
            const name = if (external_table) |table_name|
                try std.fmt.allocPrint(arena, "cdc-{s}-{s}", .{ table.name, table_name })
            else
                try std.fmt.allocPrint(arena, "cdc-{s}-{d}", .{ table.name, source_ordinal });

            const unique_name = try uniqueName(arena, used_names, name);
            var connection = Connection{
                .id = unique_name,
                .name = unique_name,
                .kind = .cdc,
                .status = cdcStatusFromReplicationStatus(status),
                .capabilities = &.{"cdc.read_stream"},
                .sources = try cdcSourcesSlice(arena, table.name, source_ordinal),
                .cdc = .{
                    .provider = provider,
                    .table_name = table.name,
                    .source_ordinal = source_ordinal,
                    .external_table = external_table,
                    .slot_name = slot_name,
                    .publication_name = publication_name,
                    .phase = phase,
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
    }
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

fn valueString(value: ?std.json.Value) ?[]const u8 {
    const actual = value orelse return null;
    return switch (actual) {
        .string => |string| if (string.len > 0) string else null,
        else => null,
    };
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
/// fanning out worker threads (capped) for live calls.
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

        const job = try arena.create(ModelsJob);
        job.* = .{
            .ep = .{
                .provider = instance.provider,
                .url = instance.url,
                .api_key = instance.api_key,
                .region = instance.region,
                .project_id = instance.project_id,
                .location = instance.location,
                .credentials_path = instance.credentials_path,
            },
            .timeout_ms = opts.timeout_ms,
            .arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
        try pending.append(arena, .{ .index = i, .job = job });
    }

    // Run pending jobs in capped batches.
    const max_workers = @min(@max(opts.max_workers, 1), 64);
    var offset: usize = 0;
    while (offset < pending.items.len) {
        const end = @min(pending.items.len, offset + max_workers);
        var threads: [64]?std.Thread = @splat(null);
        for (pending.items[offset..end], 0..) |item, slot| {
            threads[slot] = std.Thread.spawn(.{}, ModelsJob.run, .{item.job}) catch |err| blk: {
                item.job.err = err;
                break :blk null;
            };
        }
        for (threads[0 .. end - offset]) |maybe_thread| {
            if (maybe_thread) |thread| thread.join();
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
            outcome = .{ .ok = false, .err_name = @errorName(item.job.err orelse error.Unknown) };
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

fn appendExternalIoConnections(
    arena: Allocator,
    connections: *std.ArrayListUnmanaged(Connection),
    used_names: *std.StringArrayHashMapUnmanaged(void),
    node_config: *const common_config.Config,
    cache: ?*Cache,
    opts: BuildOptions,
) !void {
    if (node_config.storage.s3_bucket) |bucket| {
        const buckets = try arena.alloc([]const u8, 1);
        buckets[0] = bucket;
        const name = try uniqueName(arena, used_names, "storage");
        try connections.append(arena, .{
            .id = name,
            .name = name,
            .kind = .external_io,
            .status = .configured,
            .capabilities = &.{ "objects.read", "objects.write", "backup.write", "restore.read" },
            .sources = try sourcesSlice(arena, "config:storage/{s}", "s3"),
            .external_io = .{
                .protocol = .s3,
                .buckets = buckets,
                .prefix = node_config.storage.s3_prefix,
            },
        });
    }

    if (node_config.inference.s3_credentials) |creds| {
        const name = try uniqueName(arena, used_names, "inference-models");
        try connections.append(arena, .{
            .id = name,
            .name = name,
            .kind = .external_io,
            .status = .configured,
            .capabilities = &.{ "objects.read", "models.load" },
            .sources = try sourcesSlice(arena, "config:inference/{s}", "s3_credentials"),
            .external_io = .{
                .protocol = .s3,
                .endpoint = creds.endpoint,
            },
        });
    }

    if (node_config.remote_content) |remote_content| {
        var it = remote_content.s3.iterator();
        while (it.next()) |entry| {
            const creds = entry.value_ptr.*;
            const name = try uniqueName(arena, used_names, entry.key_ptr.*);
            var connection = Connection{
                .id = name,
                .name = name,
                .kind = .external_io,
                .status = .configured,
                .capabilities = &.{ "content.fetch", "objects.read", "indexing.use", "agents.use" },
                .sources = try sourcesSlice(arena, "config:remote_content/s3/{s}", entry.key_ptr.*),
                .external_io = .{
                    .protocol = .s3,
                    .endpoint = creds.endpoint,
                    .buckets = creds.buckets orelse &.{},
                },
            };
            if (opts.probe) {
                if (try probeRemoteContentS3(arena, entry.key_ptr.*, creds, cache, opts)) |probe| {
                    connection.status = probe.status;
                    connection.@"error" = probe.err_name;
                }
            }
            try connections.append(arena, connection);
        }

        var http_it = remote_content.http.iterator();
        while (http_it.next()) |entry| {
            var hosts = std.ArrayListUnmanaged([]const u8).empty;
            if (entry.value_ptr.base_url) |base_url| try hosts.append(arena, base_url);
            const name = try uniqueName(arena, used_names, entry.key_ptr.*);
            try connections.append(arena, .{
                .id = name,
                .name = name,
                .kind = .external_io,
                .status = .configured,
                .capabilities = &.{ "content.fetch", "indexing.use", "agents.use" },
                .sources = try sourcesSlice(arena, "config:remote_content/http/{s}", entry.key_ptr.*),
                .external_io = .{
                    .protocol = .http,
                    .hosts = hosts.items,
                },
            });
        }
    }
}

const ProbeResult = struct {
    status: ConnectionStatus,
    err_name: ?[]const u8 = null,
};

/// Probe a remote-content S3 connection by checking its first bucket.
/// Returns null when the credentials are incomplete (no probe possible).
fn probeRemoteContentS3(
    arena: Allocator,
    name: []const u8,
    creds: common_config.Config.S3CredentialConfig,
    cache: ?*Cache,
    opts: BuildOptions,
) !?ProbeResult {
    const endpoint = creds.endpoint orelse return null;
    const access_key_id = creds.access_key_id orelse return null;
    const secret_access_key = creds.secret_access_key orelse return null;
    const buckets = creds.buckets orelse return null;
    if (buckets.len == 0) return null;

    const cache_key = try std.fmt.allocPrint(arena, "objectstore\x1f{s}\x1f{s}\x1f{s}", .{ name, endpoint, buckets[0] });
    const now_ns = platform_time.monotonicNs();
    if (!opts.refresh) {
        if (cache) |c| {
            if (try c.lookupCopy(arena, cache_key, now_ns, opts.ttl_ns)) |entry| {
                return .{
                    .status = if (entry.ok) .connected else .@"error",
                    .err_name = if (entry.ok) null else entry.err_name,
                };
            }
        }
    }

    const outcome: ProbeResult = blk: {
        probeS3Bucket(arena, creds, endpoint, access_key_id, secret_access_key, buckets[0]) catch |err| {
            break :blk .{ .status = .@"error", .err_name = @errorName(err) };
        };
        break :blk .{ .status = .connected };
    };

    if (cache) |c| {
        c.store(cache_key, .{
            .captured_at_ns = now_ns,
            .ok = outcome.status == .connected,
            .err_name = @constCast(outcome.err_name orelse ""),
        }) catch |err| {
            std.log.warn("connections: probe cache store failed err={}", .{err});
        };
    }
    return outcome;
}

fn probeS3Bucket(
    arena: Allocator,
    creds: common_config.Config.S3CredentialConfig,
    endpoint: []const u8,
    access_key_id: []const u8,
    secret_access_key: []const u8,
    bucket: []const u8,
) !void {
    var s3_client = try objectstore.s3.Client.init(arena, .{
        .credentials = .{
            .endpoint = try arena.dupe(u8, endpoint),
            .use_ssl = creds.use_ssl orelse std.mem.startsWith(u8, endpoint, "https://"),
            .access_key_id = try arena.dupe(u8, access_key_id),
            .secret_access_key = try arena.dupe(u8, secret_access_key),
            .session_token = if (creds.session_token) |value| try arena.dupe(u8, value) else null,
            .region = try arena.dupe(u8, "us-east-1"),
        },
        .addressing_style = .path,
    });
    var client = s3_client.client();
    defer client.deinit();
    const exists = try client.bucketExists(bucket);
    if (!exists) return error.BucketNotFound;
}

// --- Tests ---

const table_manager = @import("../metadata/table_manager.zig");

test "gather dedups registry instances by provider identity" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const raw =
        \\{
        \\  "generators": {
        \\    "primary": { "provider": "openai", "model": "gpt-4o", "url": "https://api.openai.com", "api_key": "k1" },
        \\    "secondary": { "provider": "openai", "model": "gpt-4o-mini", "url": "https://api.openai.com", "api_key": "k1" }
        \\  },
        \\  "embedders": {
        \\    "embed": { "provider": "openai", "model": "text-embedding-3-small", "url": "https://api.openai.com", "api_key": "k1" }
        \\  },
        \\  "rerankers": {
        \\    "rerank": { "provider": "cohere", "model": "rerank-v3.5", "api_key": "k2", "field": "body" }
        \\  }
        \\}
    ;
    var registry = try provider_registry.Registry.parseFromSlice(alloc, raw);
    defer registry.deinit();

    var state = try gatherInstances(arena, .{ .registry = &registry });
    const instances = state.instances.values();
    try std.testing.expectEqual(@as(usize, 2), instances.len);

    const openai_instance = instances[0];
    try std.testing.expectEqual(list_models.ProviderTag.openai, openai_instance.provider);
    try std.testing.expectEqual(@as(usize, 3), openai_instance.names.items.len);
    try std.testing.expect(openai_instance.model_types.contains(.generator));
    try std.testing.expect(openai_instance.model_types.contains(.embedder));
    try std.testing.expect(!openai_instance.model_types.contains(.reranker));
    try std.testing.expect(openai_instance.configured_models.contains("gpt-4o"));
    try std.testing.expect(openai_instance.configured_models.contains("text-embedding-3-small"));

    const cohere_instance = instances[1];
    try std.testing.expectEqual(list_models.ProviderTag.cohere, cohere_instance.provider);
    try std.testing.expect(cohere_instance.model_types.contains(.reranker));
}

test "gather includes table embedding index configs" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var tables = [_]table_manager.TableRecord{.{
        .table_id = 1,
        .name = "docs",
        .indexes_json =
        \\{"body_vec":{"type":"embeddings","field":"body","dimension":768,"embedder":{"provider":"ollama","model":"nomic-embed-text","url":"http://localhost:11434"}},"broken":{"type":"embeddings","embedder":{"provider":"unknown-provider"}},"ft":{"type":"full_text"}}
        ,
    }};
    var snapshot = metadata_api.AdminSnapshot{
        .status = undefined,
        .tables = tables[0..],
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };

    var state = try gatherInstances(arena, .{ .snapshot = &snapshot });
    const instances = state.instances.values();
    try std.testing.expectEqual(@as(usize, 1), instances.len);
    try std.testing.expectEqual(list_models.ProviderTag.ollama, instances[0].provider);
    try std.testing.expectEqual(@as(usize, 1), instances[0].sources.items.len);
    try std.testing.expectEqualStrings("table:docs/index:body_vec", instances[0].sources.items[0]);
}

test "build response reports mock connected and types filter" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const raw =
        \\{ "generators": { "mocked": { "provider": "mock" } } }
    ;
    var registry = try provider_registry.Registry.parseFromSlice(alloc, raw);
    defer registry.deinit();

    const response = try buildConnectionsResponse(arena, .{ .registry = &registry }, null, .{ .include_models = true });
    try std.testing.expectEqual(@as(usize, 1), response.connections.len);
    const connection = response.connections[0];
    try std.testing.expectEqualStrings("mocked", connection.name);
    try std.testing.expectEqual(ConnectionStatus.connected, connection.status);
    const models = connection.inference.?.models.?;
    try std.testing.expect(models.map.get("embedders") != null);
    try std.testing.expect(models.map.get("generators") != null);

    const filtered = try buildConnectionsResponse(arena, .{ .registry = &registry }, null, .{ .types_filter = "object_store" });
    try std.testing.expectEqual(@as(usize, 0), filtered.connections.len);
}

test "build response reports object and remote content config as external io" {
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
        \\  "storage": {
        \\    "data": "s3",
        \\    "s3": { "bucket": "antfly-prod", "prefix": "cluster-a/" }
        \\  },
        \\  "remote_content": {
        \\    "s3": {
        \\      "docs": {
        \\        "endpoint": "s3.amazonaws.com",
        \\        "buckets": ["docs-*"]
        \\      }
        \\    },
        \\    "http": {
        \\      "docs-site": {
        \\        "base_url": "https://docs.example.com"
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
        .{ .types_filter = "external_io", .probe = false },
    );
    try std.testing.expectEqual(@as(usize, 3), response.connections.len);
    for (response.connections) |connection| {
        try std.testing.expectEqual(ConnectionKind.external_io, connection.kind);
        try std.testing.expect(connection.id.len > 0);
        try std.testing.expect(connection.external_io != null);
    }

    const storage = response.connections[0];
    try std.testing.expectEqual(ExternalIoProtocol.s3, storage.external_io.?.protocol);
    try std.testing.expect(containsString(storage.capabilities, "objects.read"));
    try std.testing.expect(containsString(storage.capabilities, "backup.write"));

    const docs_s3 = response.connections[1];
    try std.testing.expectEqual(ExternalIoProtocol.s3, docs_s3.external_io.?.protocol);
    try std.testing.expect(containsString(docs_s3.capabilities, "content.fetch"));
    try std.testing.expectEqualStrings("docs-*", docs_s3.external_io.?.buckets[0]);

    const docs_http = response.connections[2];
    try std.testing.expectEqual(ExternalIoProtocol.http, docs_http.external_io.?.protocol);
    try std.testing.expect(containsString(docs_http.capabilities, "content.fetch"));
    try std.testing.expectEqualStrings("https://docs.example.com", docs_http.external_io.?.hosts[0]);

    const alias_response = try buildConnectionsResponse(
        arena,
        .{ .node_config = &cfg },
        null,
        .{ .types_filter = "object_store", .probe = false },
    );
    try std.testing.expectEqual(@as(usize, 3), alias_response.connections.len);
    try std.testing.expectEqual(ConnectionKind.external_io, alias_response.connections[0].kind);
}

test "build response includes cdc replication sources with generic cdc kind" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var tables = [_]table_manager.TableRecord{.{
        .table_id = 7,
        .name = "users",
        .replication_sources_json =
        \\[{"type":"postgres","dsn":"${secret:pg}","postgres_table":"public.users","slot_name":"slot_cfg","publication_name":"pub_cfg"}]
        ,
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

    const response = try buildConnectionsResponse(arena, .{ .snapshot = &snapshot }, null, .{ .types_filter = "cdc" });
    try std.testing.expectEqual(@as(usize, 1), response.connections.len);
    const connection = response.connections[0];
    try std.testing.expectEqual(ConnectionKind.cdc, connection.kind);
    try std.testing.expectEqual(ConnectionStatus.connected, connection.status);
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
}

test "unique names get numeric suffixes" {
    const alloc = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var used = std.StringArrayHashMapUnmanaged(void){};
    try std.testing.expectEqualStrings("openai", try uniqueName(arena, &used, "openai"));
    try std.testing.expectEqualStrings("openai-2", try uniqueName(arena, &used, "openai"));
    try std.testing.expectEqualStrings("openai-3", try uniqueName(arena, &used, "openai"));
}
