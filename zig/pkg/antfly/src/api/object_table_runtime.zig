// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Object-backed document tables hosted by a native API process. The native
//! catalog owns table existence; the object runtime owns WAL and published HEAD.
//! Every incarnation has a private catalog projection, never a second public DDL
//! authority. External lake tables use native lake publication instead.
const std = @import("std");
const local = @import("antfly_local_sources");
const bootstrap = @import("../serverless/runtime/bootstrap.zig");
const http = @import("../serverless/api/http_types.zig");
const Store = @import("lake_index_store.zig").Store;
const metadata = @import("../metadata/table_manager.zig");
const A = std.mem.Allocator;
pub fn storeIdentity(a: A, store: *const Store) ![32]u8 {
    const encoded = try std.json.Stringify.valueAlloc(a, store.locator, .{});
    defer a.free(encoded);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
    return digest;
}

pub const Options = struct {
    config: ?*const local.common_config.Config = null,
    secrets: ?*local.common_secrets.FileStore = null,
    deployment: local.common_config.DeploymentMode,
    local_base_dir: ?[]const u8 = null,
    graph_execution_limits: local.graph_work_budget.Limits = .{},
    remote_content: ?*const @import("antfly_scraping").RemoteContentConfig = null,
};

const Entry = struct {
    store: Store,
    stack: bootstrap.OwnedStack,
    schema: []u8,
    indexes: []u8,
    storage_identity: [32]u8,
    fn init(self: *Entry, a: A, io: std.Io, table: metadata.TableRecord, options: Options, context: local.api_operation.RequestContext) !void {
        try context.ensureActive();
        self.storage_identity = table.object_storage_identity;
        self.schema = try a.dupe(u8, table.schema_json);
        errdefer a.free(self.schema);
        self.indexes = try local.api_index_metadata.aliases.physicalProjection(a, table.indexes_json);
        errdefer a.free(self.indexes);
        self.store = try Store.openNative(a, options.config, options.secrets, false, options.deployment, options.local_base_dir);
        errdefer self.store.deinit();
        if (!std.mem.eql(u8, &table.object_storage_identity, &(try storeIdentity(a, &self.store)))) return error.ObjectTableStorageBindingChanged;
        const prefix = try std.fmt.allocPrint(a, "{s}{s}object-tables/{d}/{d}", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/", table.table_id, table.object_storage_generation });
        defer a.free(prefix);
        // URI metadata is descriptive; all I/O uses the already authorized client.
        const uri = try std.fmt.allocPrint(a, "s3://{s}/{s}", .{ self.store.opened.bucket, prefix });
        defer a.free(uri);
        try context.ensureActive();
        try self.stack.init(a, .{
            .native_location = .{ .client = self.store.opened.client, .bucket = self.store.opened.bucket, .prefix = prefix },
            .artifacts_uri = uri,
            .manifests_uri = uri,
            .wal_uri = uri,
            .progress_uri = uri,
            .catalog_uri = uri,
            .combined_mode = true,
            .node_config = options.config,
            .secret_store = options.secrets,
            .graph_execution_limits = options.graph_execution_limits,
            .remote_content = options.remote_content,
        }, io);
        errdefer self.stack.deinit();
        try context.ensureActive();
        _ = try self.stack.catalog.ensureTableWithDefinition("data", 0, .{}, table.schema_json, table.read_schema_json, self.indexes);
        try context.ensureActive();
        var recorded = (try self.stack.catalog.getTableAlloc(a, "data")) orelse return error.ObjectTableDefinitionConflict;
        defer recorded.deinit(a);
        if (!std.mem.eql(u8, recorded.schema_json, table.schema_json) or !try local.api_index_metadata.aliases.configurationEqual(a, recorded.indexes_json, self.indexes)) return error.ObjectTableDefinitionConflict;
        try context.ensureActive();
        try self.stack.runtime.start();
    }
    fn deinit(self: *Entry, a: A) void {
        self.stack.deinit();
        self.store.deinit();
        a.free(self.schema);
        a.free(self.indexes);
    }
};

pub const Manager = struct {
    mutex: std.Io.Mutex = .init,
    entries: std.AutoHashMapUnmanaged([2]u64, *Entry) = .empty,
    /// Bound runtime/client overhead; admission fails before opening more stores.
    pub const max_tables = 128;
    pub fn deinit(self: *Manager, a: A) void {
        var values = self.entries.valueIterator();
        while (values.next()) |entry| {
            entry.*.deinit(a);
            a.destroy(entry.*);
        }
        self.entries.deinit(a);
    }
    fn acquire(self: *Manager, a: A, io: std.Io, table: metadata.TableRecord, options: Options, context: local.api_operation.RequestContext) !*Entry {
        while (!self.mutex.tryLock()) {
            try context.ensureActive();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        defer self.mutex.unlock(io);
        try context.ensureActive();
        const key = [2]u64{ table.table_id, table.object_storage_generation };
        if (self.entries.get(key)) |entry| {
            // Only authoritative native bindings enter this manager. Metadata
            // publication rejects every physical recipe change within an object
            // generation (validateObjectTableMutation), before replication and
            // projection. Routing/receipt updates cannot change this runtime.
            // Use that immutable identity rather than reparsing a catalog while
            // holding the manager-wide lock on every request.
            if (!std.mem.eql(u8, &entry.storage_identity, &table.object_storage_identity)) return error.ObjectTableStorageBindingChanged;
            if (!std.mem.eql(u8, entry.schema, table.schema_json)) return error.ObjectTableDefinitionConflict;
            return entry;
        }
        if (self.entries.count() >= max_tables) return error.ObjectTableRuntimeCapacityExceeded;
        const entry = try a.create(Entry);
        errdefer a.destroy(entry);
        try entry.init(a, io, table, options, context);
        errdefer entry.deinit(a);
        try context.ensureActive();
        try self.entries.put(a, key, entry);
        return entry;
    }
    /// Caller has resolved and authorized a fresh native catalog binding.
    pub fn handle(self: *Manager, a: A, io: std.Io, table: metadata.TableRecord, options: Options, method: @import("../serverless/api/http_routes.zig").HttpMethod, suffix: []const u8, body: []const u8, context: local.api_operation.RequestContext) !http.HttpResponse {
        const guard = http.RequestGuard{ .context = try context.platformDeadline() };
        const cancellation = guard.token();
        try guard.context.ensureActive();
        const entry = try self.acquire(a, io, table, options, guard.context);
        try guard.context.ensureActive();
        if (method == .get and std.mem.eql(u8, suffix, "lookup")) {
            var table_binding = (try entry.stack.catalog.getTableAlloc(a, "data")) orelse return error.TableNotFound;
            defer table_binding.deinit(a);
            var session = entry.stack.query.openHeadSessionWithCancellation(table_binding.namespace, cancellation) catch |err| switch (err) {
                error.FileNotFound, error.NotFound => return .{ .status = 404, .content_type = try a.dupe(u8, "text/plain"), .body = try a.dupe(u8, "not found") },
                else => return err,
            };
            defer session.deinit();
            try guard.context.ensureActive();
            session.setCancellation(cancellation);
            var remaining: u64 = 64 * 1024 * 1024;
            const reader = (@import("../serverless/query/document_facts_reader.zig").Reader.create(a, &session, &remaining) catch |err| {
                try guard.context.ensureActive();
                return err;
            }) orelse return error.ObjectTableDocumentFactsUnavailable;
            defer reader.destroy();
            const found = reader.lookup(body) catch |err| {
                try guard.context.ensureActive();
                return err;
            };
            try guard.context.ensureActive();
            const fact = found orelse return .{ .status = 404, .content_type = try a.dupe(u8, "text/plain"), .body = try a.dupe(u8, "not found") };
            const value = reader.readBodyAlloc(fact) catch |err| {
                try guard.context.ensureActive();
                return err;
            };
            errdefer a.free(value);
            try guard.context.ensureActive();
            return .{ .status = 200, .content_type = try a.dupe(u8, "application/json"), .body = value };
        }
        const path = try std.fmt.allocPrint(a, "/tables/data/{s}", .{suffix});
        defer a.free(path);
        var bound_body: ?[]u8 = null;
        defer if (bound_body) |bytes| a.free(bytes);
        if (std.mem.eql(u8, suffix, "query")) bound_body = try stripAliasFences(a, body, entry.indexes);
        var response = entry.stack.handler.handle(.{ .method = method, .path = path, .body = bound_body orelse body, .cancellation = context.cancellation, .deadline_ns = guard.context.deadline_ns }) catch |err| {
            // Transport-only cancellation adapters may report Canceled when
            // the request deadline expired. Preserve the native error kind.
            if (method == .get or std.mem.eql(u8, suffix, "query")) try guard.context.ensureActive();
            return err;
        };
        errdefer response.deinit(a);
        // The embedded HTTP adapter can encode generic cancellation as a
        // response. Reads still belong to this native request's precise budget.
        // Mutation responses retain their admitted durable outcome unchanged.
        if (method == .get or std.mem.eql(u8, suffix, "query")) try guard.context.ensureActive();
        return response;
    }
};

/// Only the admitted internal envelope may contain fences. Still decode it
/// defensively: malformed control state is an error, never a union-tag trap.
fn stripAliasFences(a: A, body: []const u8, physical_indexes: []const u8) !?[]u8 {
    if (std.mem.indexOf(u8, body, "_index_alias_fences") == null and std.mem.indexOfScalar(u8, body, '\\') == null) return null;
    var parsed = std.json.parseFromSlice(std.json.Value, a, body, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    const raw = parsed.value.object.get("_index_alias_fences") orelse return null;
    var fences = std.json.parseFromValue([]const local.storage_db_types.IndexAliasFence, a, raw, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidQueryRequest;
    defer fences.deinit();
    if (fences.value.len == 0 or fences.value.len > local.api_index_metadata.aliases.max_aliases_per_table) return error.InvalidQueryRequest;
    var catalog = try std.json.parseFromSlice(std.json.Value, a, physical_indexes, .{});
    defer catalog.deinit();
    if (catalog.value != .object) return error.InvalidTableIndexMetadata;
    for (fences.value) |fence| {
        if (fence.name.len == 0 or fence.name.len > 256 or fence.incarnation == 0) return error.InvalidQueryRequest;
        const config = catalog.value.object.get(fence.name) orelse return error.TableGenerationChanged;
        const identity = (try @import("indexes.zig").indexRuntimeIdentity(a, fence.name, config)) orelse return error.TableGenerationChanged;
        if (identity.incarnation != fence.incarnation) return error.TableGenerationChanged;
    }
    _ = parsed.value.object.orderedRemove("_index_alias_fences");
    return try std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

test "index alias object internal fences validate malformed and replaced identities" {
    const a = std.testing.allocator;
    const indexes = "{\"text\":{\"type\":\"full_text\",\"_index_incarnation\":41}}";
    for ([_][]const u8{
        "{\"_index_alias_fences\":1}",
        "{\"_index_alias_fences\":{}}",
        "{\"_index_alias_fences\":[null]}",
        "{\"_index_alias_fences\":[{}]}",
        "{\"_index_alias_fences\":[{\"name\":1,\"incarnation\":41}]}",
        "{\"_index_alias_fences\":[{\"name\":\"text\",\"incarnation\":-1}]}",
        "{\"_index_alias_fences\":[{\"name\":\"text\",\"incarnation\":0}]}",
    }) |body| try std.testing.expectError(error.InvalidQueryRequest, stripAliasFences(a, body, indexes));
    try std.testing.expectError(error.TableGenerationChanged, stripAliasFences(a, "{\"_index_alias_fences\":[{\"name\":\"text\",\"incarnation\":42}]}", indexes));
    const body = (try stripAliasFences(a, "{\"indexes\":[\"text\"],\"_index_alias_fences\":[{\"name\":\"text\",\"incarnation\":41}]}", indexes)).?;
    defer a.free(body);
    try std.testing.expectEqualStrings("{\"indexes\":[\"text\"]}", body);
}

test "index alias object runtime reuse relies on publication immutable recipe without allocations" {
    const a = std.testing.allocator;
    const before: metadata.TableRecord = .{ .table_id = 7, .name = "docs", .storage = .{ .engine = .object }, .object_storage_generation = 1, .object_storage_identity = @splat(1), .schema_json = "{}", .indexes_json = "{\"text\":{\"type\":\"full_text\",\"_index_incarnation\":41}}", .desired_replica_count = 0, .min_ranges = 0 };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ready = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"complete\":true}", .{});
    var after = before;
    after.indexes_json = try local.api_index_metadata.aliases.put(arena.allocator(), before.indexes_json, "live", .{ .index = "text", .idempotency_key = "1" }, ready, 1);
    try metadata.validateObjectTableMutation(a, before, after);
    var invalid = after;
    invalid.indexes_json = "{\"other\":{\"type\":\"full_text\"}}";
    try std.testing.expectError(error.ObjectTableDefinitionConflict, metadata.validateObjectTableMutation(a, before, invalid));
    var entry: Entry = .{ .store = undefined, .stack = undefined, .schema = @constCast(before.schema_json), .indexes = @constCast(before.indexes_json), .storage_identity = before.object_storage_identity };
    var manager: Manager = .{};
    defer manager.entries.deinit(a);
    try manager.entries.put(a, .{ before.table_id, before.object_storage_generation }, &entry);
    // An already-published incarnation must need no allocation or JSON parse.
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectEqual(&entry, try manager.acquire(failing.allocator(), std.testing.io, after, .{ .deployment = .standalone }, .{}));
    invalid = after;
    invalid.object_storage_identity = @splat(2);
    try std.testing.expectError(error.ObjectTableStorageBindingChanged, manager.acquire(failing.allocator(), std.testing.io, invalid, .{ .deployment = .standalone }, .{}));
}
