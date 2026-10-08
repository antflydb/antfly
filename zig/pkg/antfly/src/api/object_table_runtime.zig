// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
};

const Entry = struct {
    store: Store,
    stack: bootstrap.OwnedStack,
    schema: []u8,
    indexes: []u8,
    fn init(self: *Entry, a: A, io: std.Io, table: metadata.TableRecord, options: Options) !void {
        self.schema = try a.dupe(u8, table.schema_json);
        errdefer a.free(self.schema);
        self.indexes = try a.dupe(u8, table.indexes_json);
        errdefer a.free(self.indexes);
        self.store = try Store.openNative(a, options.config, options.secrets, false, options.deployment, options.local_base_dir);
        errdefer self.store.deinit();
        if (!std.mem.eql(u8, &table.object_storage_identity, &(try storeIdentity(a, &self.store)))) return error.ObjectTableStorageBindingChanged;
        const prefix = try std.fmt.allocPrint(a, "{s}{s}object-tables/{d}/{d}", .{ self.store.opened.prefix, if (self.store.opened.prefix.len == 0) "" else "/", table.table_id, table.object_storage_generation });
        defer a.free(prefix);
        // URI metadata is descriptive; all I/O uses the already authorized client.
        const uri = try std.fmt.allocPrint(a, "s3://{s}/{s}", .{ self.store.opened.bucket, prefix });
        defer a.free(uri);
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
        }, io);
        errdefer self.stack.deinit();
        _ = try self.stack.catalog.ensureTableWithDefinition("data", 0, .{}, table.schema_json, table.read_schema_json, table.indexes_json);
        var recorded = (try self.stack.catalog.getTableAlloc(a, "data")) orelse return error.ObjectTableDefinitionConflict;
        defer recorded.deinit(a);
        if (!std.mem.eql(u8, recorded.schema_json, table.schema_json) or !std.mem.eql(u8, recorded.indexes_json, table.indexes_json)) return error.ObjectTableDefinitionConflict;
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
    fn acquire(self: *Manager, a: A, io: std.Io, table: metadata.TableRecord, options: Options) !*Entry {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        const key = [2]u64{ table.table_id, table.object_storage_generation };
        if (self.entries.get(key)) |entry| {
            if (!std.mem.eql(u8, entry.schema, table.schema_json) or !std.mem.eql(u8, entry.indexes, table.indexes_json)) return error.ObjectTableDefinitionConflict;
            return entry;
        }
        if (self.entries.count() >= max_tables) return error.ObjectTableRuntimeCapacityExceeded;
        const entry = try a.create(Entry);
        errdefer a.destroy(entry);
        try entry.init(a, io, table, options);
        errdefer entry.deinit(a);
        try self.entries.put(a, key, entry);
        return entry;
    }
    /// Caller has resolved and authorized a fresh native catalog binding.
    pub fn handle(self: *Manager, a: A, io: std.Io, table: metadata.TableRecord, options: Options, method: @import("../serverless/api/http_routes.zig").HttpMethod, suffix: []const u8, body: []const u8, cancellation: @import("antfly_cancellation").CancellationToken) !http.HttpResponse {
        try cancellation.check();
        const entry = try self.acquire(a, io, table, options);
        if (method == .get and std.mem.eql(u8, suffix, "lookup")) {
            var table_binding = (try entry.stack.catalog.getTableAlloc(a, "data")) orelse return error.TableNotFound;
            defer table_binding.deinit(a);
            var session = entry.stack.query.openHeadSession(table_binding.namespace) catch |err| switch (err) {
                error.FileNotFound, error.NotFound => return .{ .status = 404, .content_type = try a.dupe(u8, "text/plain"), .body = try a.dupe(u8, "not found") },
                else => return err,
            };
            defer session.deinit();
            session.setCancellation(cancellation);
            var remaining: u64 = 64 * 1024 * 1024;
            const reader = (try @import("../serverless/query/document_facts_reader.zig").Reader.create(a, &session, &remaining)) orelse return error.ObjectTableDocumentFactsUnavailable;
            defer reader.destroy();
            const fact = (try reader.lookup(body)) orelse return .{ .status = 404, .content_type = try a.dupe(u8, "text/plain"), .body = try a.dupe(u8, "not found") };
            const value = try reader.readBodyAlloc(fact);
            errdefer a.free(value);
            return .{ .status = 200, .content_type = try a.dupe(u8, "application/json"), .body = value };
        }
        const path = try std.fmt.allocPrint(a, "/tables/data/{s}", .{suffix});
        defer a.free(path);
        return entry.stack.handler.handle(.{ .method = method, .path = path, .body = body, .cancellation = cancellation });
    }
};
