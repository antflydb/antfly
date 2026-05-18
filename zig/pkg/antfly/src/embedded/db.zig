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
const support = @import("embedded_support");
const db_mod = support.db;
const db_core = support.db_core;
const template_remote_host = support.template_remote_host;

pub const lsm_storage = support.lsm_storage;
pub const enrichment_runtime = support.enrichment_runtime;
pub const enrichment_embedder = support.enrichment_embedder;

const Allocator = std.mem.Allocator;
const IndexBackendOptions = @TypeOf((db_mod.OpenOptions{}).index_backends);

pub const types = support.db_types;
pub const RemoteTemplateRenderConfig = template_remote_host.RenderConfig;
pub const RemoteTemplateRenderer = template_remote_host.HostRenderer;

pub const OpenOptions = struct {
    map_size: usize = 256 * 1024 * 1024,
    no_sync: bool = false,
    primary_backend: db_mod.PrimaryBackend = .{ .lsm = .{ .flush_threshold = 1 } },
    storage: ?support.lsm_storage.Storage = null,
    index_backends: IndexBackendOptions = .{},
    enrichment: ?support.enrichment_runtime.Config = null,
};

pub const Profile = enum {
    native,
    hosted,
};

pub const Capabilities = struct {
    freestanding_build: bool = builtin.os.tag == .freestanding,
    hosted_profile: bool = false,
    manual_maintenance: bool = false,
    background_enrichment_runtime: bool = true,
    ttl_cleanup_runtime: bool = true,
    transaction_recovery_runtime: bool = true,
    local_template_rendering: bool = true,
    remote_template_rendering: bool = true,
    remote_template_host_callbacks: bool = false,
    generated_enrichment_planning: bool = true,
    dense_vector_search: bool = true,
    sparse_vector_search: bool = true,
};

pub const DB = struct {
    inner: db_mod.DB,
    profile: Profile,

    pub fn open(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try openWithProfile(alloc, path, opts, .native);
    }

    pub fn openHosted(alloc: Allocator, path: []const u8, opts: OpenOptions) !DB {
        return try openWithProfile(alloc, path, opts, .hosted);
    }

    pub fn openWithProfile(alloc: Allocator, path: []const u8, opts: OpenOptions, profile: Profile) !DB {
        return .{
            .inner = try db_mod.DB.open(alloc, path, toDbOpenOptions(opts, profile)),
            .profile = profile,
        };
    }

    pub fn close(self: *DB) void {
        self.inner.close();
        self.* = undefined;
    }

    pub fn engine(self: *DB) db_core.Engine {
        return self.inner.engine();
    }

    pub fn maintenanceDriver(self: *DB) db_core.MaintenanceDriver {
        return self.inner.maintenanceDriver();
    }

    pub fn services(self: *DB) db_core.Services {
        return self.inner.services();
    }

    pub fn capabilities(self: *DB) Capabilities {
        return capabilitiesForProfile(self.profile);
    }

    pub fn batch(self: *DB, req: types.BatchRequest) !void {
        try self.inner.batch(req);
    }

    pub fn lookup(self: *DB, alloc: Allocator, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
        return try self.inner.lookup(alloc, key, opts);
    }

    pub fn scan(self: *DB, alloc: Allocator, from_key: []const u8, to_key: []const u8, opts: types.ScanOptions) !types.ScanResult {
        return try self.inner.scan(alloc, from_key, to_key, opts);
    }

    pub fn search(self: *DB, alloc: Allocator, req: types.SearchRequest) !types.SearchResult {
        return try self.inner.search(alloc, req);
    }

    pub fn stats(self: *DB, alloc: Allocator) !types.DBStats {
        return try self.inner.stats(alloc);
    }

    pub fn listIndexes(self: *DB, alloc: Allocator) ![]types.IndexConfig {
        return try self.inner.listIndexes(alloc);
    }

    pub fn compactTextIndexes(self: *DB) !void {
        try self.inner.compactTextIndexes();
    }

    pub fn drainScheduledTextMerges(self: *DB) !void {
        try self.inner.drainScheduledTextMerges();
    }

    pub fn forceCompactTextIndexes(self: *DB) !void {
        try self.inner.forceCompactTextIndexes();
    }

    pub fn bestEffortForceCompactTextIndexes(self: *DB) !void {
        try self.inner.bestEffortForceCompactTextIndexes();
    }

    pub fn listEnrichments(self: *DB, alloc: Allocator) ![]types.EnrichmentConfig {
        return try self.inner.listEnrichments(alloc);
    }

    pub fn pendingWorkStats(self: *DB) db_core.PendingWorkStats {
        return self.maintenanceDriver().pendingWorkStats();
    }

    pub fn runUntilIdle(self: *DB) !void {
        try self.maintenanceDriver().runUntilIdle();
    }

    pub fn addIndex(self: *DB, cfg: types.IndexConfig) !void {
        try self.inner.addIndex(cfg);
    }

    pub fn addEnrichment(self: *DB, cfg: types.EnrichmentConfig) !void {
        try self.inner.addEnrichment(cfg);
    }

    pub fn setSchema(self: *DB, table_schema: support.schema.TableSchema) !void {
        try self.inner.setSchema(table_schema);
    }
};

pub fn capabilitiesForProfile(profile: Profile) Capabilities {
    const freestanding = builtin.os.tag == .freestanding;
    const hosted = profile == .hosted;
    return .{
        .hosted_profile = hosted,
        .manual_maintenance = hosted,
        .background_enrichment_runtime = !hosted and !freestanding,
        .ttl_cleanup_runtime = !hosted and !freestanding,
        .transaction_recovery_runtime = !hosted and !freestanding,
        .local_template_rendering = true,
        .remote_template_rendering = !freestanding,
        .remote_template_host_callbacks = freestanding,
        .generated_enrichment_planning = true,
        .dense_vector_search = true,
        .sparse_vector_search = true,
    };
}

pub fn setRemoteTemplateRenderer(renderer: ?RemoteTemplateRenderer) void {
    template_remote_host.setHostRenderer(renderer);
}

pub fn renderRemoteTemplateText(
    alloc: Allocator,
    template_source: []const u8,
    json_doc: []const u8,
) ![]const u8 {
    return try template_remote_host.renderJsonToText(alloc, template_source, json_doc);
}

fn toDbOpenOptions(opts: OpenOptions, profile: Profile) db_mod.OpenOptions {
    var resolved: db_mod.OpenOptions = .{
        .map_size = opts.map_size,
        .no_sync = opts.no_sync,
        .primary_backend = opts.primary_backend,
        .storage = opts.storage,
        .index_backends = opts.index_backends,
    };
    if (profile == .hosted) {
        resolved.executor = .{ .backend = .manual };
        resolved.enrichment = opts.enrichment;
        resolved.ttl_cleanup = .{ .enabled = false };
        resolved.transaction_recovery = .{ .enabled = false };
    }
    return resolved;
}
