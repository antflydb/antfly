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
    clock: catalog.Clock = .{},

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
        return .{ .context = .{ .alloc = self.alloc, .client = self.storage.opened.client, .bucket = self.storage.opened.bucket, .prefix = self.prefix, .namespace = &self.namespace, .io = io, .cancellation = .fromCallback(context, canceled) }, .dataset = self.dataset, .owner = self.writer, .clock = self.clock };
    }
    /// Bind transient catalog and object I/O to the caller's working budget.
    /// The opened transport/configuration retains its own stable lifetime.
    pub fn storeWithAllocator(self: *const Opened, a: std.mem.Allocator, io: std.Io, context: *const operation.RequestContext) catalog.Catalog {
        var result = self.store(io, context);
        result.context.alloc = a;
        return result;
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
    cycle_progress: bool = false,

    pub fn deinit(self: *ExpiryMaintenance, alloc: std.mem.Allocator) void {
        _ = self;
        _ = alloc;
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
        completion: anytype,
    ) !void {
        if (now < self.next_ns) return;
        if (config.storage.artifacts.connection == null) {
            self.next_ns = now +| std.time.ns_per_s;
            return;
        }
        const Worker = struct {
            maintenance: *ExpiryMaintenance,
            source: @TypeOf(store_),
            group: u64,
            config: *const local.common_config.Config,
            secrets: ?*local.common_secrets.FileStore,
            io: std.Io,
            context: *const operation.RequestContext,
            completion: @TypeOf(completion),
            fn next(self_: @This(), a: std.mem.Allocator, after: u64) !?local.system_catalog_domain.Resource {
                const bytes = try self_.source.retainedArchiveWorkJson(a, self_.group, after);
                return std.json.parseFromSliceLeaky(?local.system_catalog_domain.Resource, a, bytes, .{});
            }
            fn visit(self_: @This(), a: std.mem.Allocator, resource: local.system_catalog_domain.Resource) !bool {
                const result = try self_.maintenance.process(a, resource, self_.config, self_.secrets, self_.io, self_.context);
                if (result.receipt) |receipt| try self_.completion.finish(resource, receipt);
                return result.progress;
            }
        };
        return self.drain(alloc, now, context, Worker{ .maintenance = self, .source = store_, .group = group, .config = config, .secrets = secrets, .io = io, .context = context, .completion = completion });
    }

    fn drain(self: *ExpiryMaintenance, alloc: std.mem.Allocator, now: u64, context: *const operation.RequestContext, worker: anytype) !void {
        self.next_ns = now +| 10 * std.time.ns_per_ms;
        const until = @import("antfly_platform").time.monotonicNs() +| 100 * std.time.ns_per_ms;
        for (0..32) |_| {
            if (@import("antfly_platform").time.monotonicNs() >= until) break;
            try context.ensureActive();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const a = arena.allocator();
            const resource = (try worker.next(a, self.after)) orelse {
                self.next_ns = now +| std.time.ns_per_s;
                self.cycle_progress = false;
                return;
            };
            if (resource.id <= self.after) {
                if (!self.cycle_progress) {
                    self.after = 0;
                    self.next_ns = now +| std.time.ns_per_s;
                    return;
                }
                self.cycle_progress = false;
            }
            self.after = resource.id;
            const progress = worker.visit(a, resource) catch |err| {
                if (err == error.Canceled or err == error.Timeout) return err;
                if (!retryable(err)) std.log.warn("retained archive work deferred resource={d} err={s}", .{ resource.id, @errorName(err) });
                continue;
            };
            self.cycle_progress = self.cycle_progress or progress;
        }
    }

    const Outcome = struct { progress: bool = false, receipt: ?[32]u8 = null };

    fn process(_: *ExpiryMaintenance, a: std.mem.Allocator, resource: local.system_catalog_domain.Resource, config: *const local.common_config.Config, secrets: ?*local.common_secrets.FileStore, io: std.Io, context: *const operation.RequestContext) !Outcome {
        var descriptor = try local.system_catalog_retained_datasets.parse(a, resource.source_json);
        defer descriptor.deinit();
        var archive = try Opened.init(a, config, secrets, descriptor.value, false);
        defer archive.deinit();
        const archive_catalog = archive.store(io, context);
        if (descriptor.value.phase == .retiring) {
            try archive_catalog.beginDiscard(descriptor.value.definition);
            if (try archive_catalog.reclaimUpload(archive.location())) return .{ .progress = true };
            const work = (try archive_catalog.nextMaintenanceDiscard()) orelse return .{ .receipt = try archive_catalog.discardReceipt() };
            return .{ .progress = try archive_catalog.discardOne(work, archive.location()) };
        }
        if (try archive_catalog.reclaimUpload(archive.location())) return .{ .progress = true };
        const work = (try archive_catalog.nextMaintenanceExpiry()) orelse return .{};
        return .{ .progress = try archive_catalog.collectExpired(work.segment, archive.location()) };
    }

    pub fn retryable(err: anyerror) bool {
        return switch (err) {
            error.DatasetDependencyPending, error.DatasetRetentionPending, error.DatasetReadersActive, error.DatasetCompactionOrder, error.DatasetLocationUnavailable, error.PreconditionFailed => true,
            else => false,
        };
    }
};

pub fn collectionProof(a: std.mem.Allocator, resource: local.system_catalog_domain.Resource, receipt: [32]u8) !@import("../../metadata/retained_dataset_owners.zig").Collection {
    var descriptor = try local.system_catalog_retained_datasets.parse(a, resource.source_json);
    defer descriptor.deinit();
    if (descriptor.value.phase != .retiring or descriptor.value.source != null) return error.RetainedOwnerPlanChanged;
    return .{ .resource = resource.id, .generation = descriptor.value.definition.generation, .revision = descriptor.value.revision, .definition = try descriptor.value.definition.fingerprint(a), .receipt = receipt };
}

pub fn locationIdentity(storage_identity: [32]u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/retained-archive-location/v1");
    hash.update(&storage_identity);
    return hash.finalResult();
}

test "retained owner native controls archive scheduler drains fair batches and resumes after idle wrap" {
    const Worker = struct {
        visits: [2]usize = .{ 0, 0 },
        busy: bool = true,
        held: bool = false,
        fn next(_: *@This(), _: std.mem.Allocator, after: u64) !?local.system_catalog_domain.Resource {
            return .{ .id = if (after == 1) 2 else 1, .kind = .dataset, .name = "summary" };
        }
        fn visit(self: *@This(), _: std.mem.Allocator, resource: local.system_catalog_domain.Resource) !bool {
            self.visits[@intCast(resource.id - 1)] += 1;
            if (self.held and resource.id == 1) return error.DatasetReadersActive;
            return self.busy;
        }
    };
    var maintenance: ExpiryMaintenance = .{};
    defer maintenance.deinit(std.testing.allocator);
    var worker: Worker = .{};
    const context: operation.RequestContext = .{};
    try maintenance.drain(std.testing.allocator, 1000, &context, &worker);
    try std.testing.expectEqual(@as(usize, 16), worker.visits[0]);
    try std.testing.expectEqual(@as(usize, 16), worker.visits[1]);
    try std.testing.expectEqual(@as(u64, 1000 + 10 * std.time.ns_per_ms), maintenance.next_ns);
    worker.busy = false;
    try maintenance.drain(std.testing.allocator, 2000, &context, &worker);
    try std.testing.expectEqual(@as(u64, 0), maintenance.after);
    try std.testing.expectEqual(@as(u64, 2000 + std.time.ns_per_s), maintenance.next_ns);
    worker.busy = true;
    worker.held = true;
    const previous = worker.visits[1];
    try maintenance.drain(std.testing.allocator, maintenance.next_ns, &context, &worker);
    try std.testing.expect(worker.visits[1] > previous + 1);
}
