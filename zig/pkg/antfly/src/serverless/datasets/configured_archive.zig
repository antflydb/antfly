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

//! Open the same configured archive from data publishers and metadata
//! verification. The descriptor pins the physical destination across retries.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = @import("catalog.zig");
const artifact_store = @import("../../api/lake_index_store.zig");
const operation = local.api_operation;

pub const Opened = struct {
    alloc: std.mem.Allocator,
    storage: artifact_store.Store,
    prefix: []const u8,
    namespace: [64]u8,
    dataset: [32]u8,
    location_id: [32]u8,
    writer: [16]u8,

    pub fn init(a: std.mem.Allocator, config: *const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, descriptor: local.system_catalog_retained_datasets.Descriptor, read_only: bool) !Opened {
        var storage = try artifact_store.Store.open(a, config, secrets, read_only);
        errdefer storage.deinit();
        if (storage.locator.protocol == .filesystem) return error.InvalidDatasetBinding;
        const id = locationIdentity(storage.identity);
        if (!std.mem.eql(u8, &id, &descriptor.catalog_location) or !std.mem.eql(u8, &storage.identity, &descriptor.catalog_storage_identity)) return error.InvalidDatasetBinding;
        const prefix = try std.fmt.allocPrint(a, "{s}/retained", .{storage.opened.prefix});
        errdefer a.free(prefix);
        var authority = std.crypto.hash.sha2.Sha256.init(.{});
        authority.update("antfly/retained-archive-writer/v1");
        authority.update(&descriptor.definition.id);
        authority.update(&(descriptor.definition.summary orelse return error.InvalidDatasetDefinition).source);
        const writer = authority.finalResult();
        return .{ .alloc = a, .storage = storage, .prefix = prefix, .namespace = std.fmt.bytesToHex(descriptor.definition.id, .lower), .dataset = descriptor.definition.id, .location_id = id, .writer = writer[0..16].* };
    }
    pub fn deinit(self: *Opened) void {
        self.alloc.free(self.prefix);
        self.storage.deinit();
    }
    pub fn store(self: *const Opened, io: std.Io, context: *const operation.RequestContext) catalog.Catalog {
        return .{ .context = .{ .alloc = self.alloc, .client = self.storage.opened.client, .bucket = self.storage.opened.bucket, .prefix = self.prefix, .namespace = &self.namespace, .io = io, .cancellation = .fromCallback(context, canceled) }, .dataset = self.dataset, .owner = self.writer };
    }
    pub fn location(self: *const Opened) catalog.Location {
        return .{ .id = self.location_id, .storage_identity = self.storage.identity, .client = self.storage.opened.client, .bucket = self.storage.opened.bucket, .prefix = self.prefix, .access = .queryable };
    }
    fn canceled(raw: *const anyopaque) bool {
        const context: *const operation.RequestContext = @ptrCast(@alignCast(raw));
        context.ensureActive() catch return true;
        return false;
    }
};

/// The metadata authority drives a bounded indexed expiry sweep. The resume
/// positions are scheduling hints; durable archive state governs collection.
pub const ExpiryMaintenance = struct {
    after: u64 = 0,
    next_ns: u64 = 0,
    cursors: std.AutoHashMapUnmanaged(u64, catalog.ExpiryCursor) = .empty,

    pub fn deinit(self: *ExpiryMaintenance, alloc: std.mem.Allocator) void {
        self.cursors.deinit(alloc);
    }

    pub fn run(
        self: *ExpiryMaintenance,
        alloc: std.mem.Allocator,
        store_: anytype,
        group: u64,
        config: *const local.common_config.Config,
        secrets: ?*local.common_secrets.FileStore,
        io: std.Io,
        now: u64,
        context: *const operation.RequestContext,
    ) !void {
        if (now < self.next_ns or config.storage.artifacts.connection == null) return;
        self.next_ns = now +| 2 * std.time.ns_per_s;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const bytes = try store_.retainedArchiveWorkJson(a, group, self.after);
        const resource = (try std.json.parseFromSliceLeaky(?local.system_catalog_domain.Resource, a, bytes, .{})) orelse {
            self.next_ns = now +| 15 * std.time.ns_per_s;
            return;
        };
        self.after = resource.id;
        var descriptor = try local.system_catalog_retained_datasets.parse(a, resource.source_json);
        defer descriptor.deinit();
        var archive = try Opened.init(a, config, secrets, descriptor.value, false);
        defer archive.deinit();
        const archive_catalog = archive.store(io, context);
        const after = self.cursors.get(resource.id);
        const work = (try archive_catalog.nextExpiry(after)) orelse {
            _ = self.cursors.remove(resource.id);
            return;
        };
        if (self.cursors.count() >= 4096) self.cursors.clearRetainingCapacity();
        try self.cursors.put(alloc, resource.id, work.cursor);
        try context.ensureActive();
        try archive_catalog.collectExpired(work.segment, archive.location());
    }

    pub fn retryable(err: anyerror) bool {
        return switch (err) {
            error.DatasetDependencyPending, error.DatasetRetentionPending, error.DatasetReadersActive, error.DatasetCompactionOrder, error.DatasetLocationUnavailable => true,
            else => false,
        };
    }
};

pub fn locationIdentity(storage_identity: [32]u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/retained-archive-location/v1");
    hash.update(&storage_identity);
    return hash.finalResult();
}
