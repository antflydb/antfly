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
const db_enrichment_executor = @import("db_enrichment_executor.zig");
const fs_paths = @import("../common/fs_paths.zig");
const db_mod = @import("../storage/db/db.zig");

pub const GroupDbPathResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve_path: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) anyerror![]u8,
    };

    pub fn resolvePath(self: GroupDbPathResolver, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        return try self.vtable.resolve_path(self.ptr, alloc, group_id);
    }
};

pub const OpenDbRuntimeFactoryConfig = struct {
    open_options: db_mod.OpenOptions,
    owner_id: ?[]const u8 = null,
};

pub const OpenDbRuntimeFactory = struct {
    alloc: std.mem.Allocator,
    cfg: OpenDbRuntimeFactoryConfig,
    resolver: GroupDbPathResolver,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: OpenDbRuntimeFactoryConfig,
        resolver: GroupDbPathResolver,
    ) OpenDbRuntimeFactory {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .resolver = resolver,
        };
    }

    pub fn iface(self: *OpenDbRuntimeFactory) db_enrichment_executor.GroupRuntimeFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .start_runtime = startRuntime,
            },
        };
    }

    fn startRuntime(ptr: *anyopaque, group_id: u64) !db_enrichment_executor.GroupRuntimeHandle {
        const self: *OpenDbRuntimeFactory = @ptrCast(@alignCast(ptr));
        const open_options = self.effectiveOpenOptions();
        if (open_options.enrichment == null) return error.MissingDbEnrichmentConfig;

        const path = try self.resolver.resolvePath(self.alloc, group_id);
        defer self.alloc.free(path);
        var io_impl = std.Io.Threaded.init(self.alloc, .{});
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), path);

        const db = try self.alloc.create(db_mod.DB);
        errdefer self.alloc.destroy(db);
        db.* = try db_mod.DB.open(self.alloc, path, open_options);
        errdefer db.close();

        if (db.enrichment_runtime == null) {
            db.close();
            return error.MissingDbEnrichmentRuntime;
        }

        const handle = try self.alloc.create(DbRuntimeHandle);
        handle.* = .{
            .alloc = self.alloc,
            .db = db,
        };
        return handle.handle();
    }

    fn effectiveOpenOptions(self: *const OpenDbRuntimeFactory) db_mod.OpenOptions {
        var opts = self.cfg.open_options;
        if (self.cfg.owner_id) |owner_id| {
            if (opts.enrichment) |*enrichment| {
                enrichment.owner_id = owner_id;
            }
        }
        return opts;
    }
};

const DbRuntimeHandle = struct {
    alloc: std.mem.Allocator,
    db: ?*db_mod.DB,

    fn handle(self: *@This()) db_enrichment_executor.GroupRuntimeHandle {
        return .{
            .ptr = self,
            .vtable = &.{
                .stop = stop,
                .deinit = deinit,
            },
        };
    }

    fn stop(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.db) |db| {
            db.close();
            self.alloc.destroy(db);
            self.db = null;
        }
    }

    fn deinit(ptr: *anyopaque, alloc: std.mem.Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.db) |db| {
            db.close();
            alloc.destroy(db);
            self.db = null;
        }
        alloc.destroy(self);
    }
};

test "open db runtime factory starts real db enrichment runtime handles" {
    const embedder_mod = @import("../storage/db/enrichment/embedder.zig");

    const Resolver = struct {
        root: []const u8,

        fn iface(self: *@This()) GroupDbPathResolver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve_path = resolvePath,
                },
            };
        }

        fn resolvePath(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ self.root, group_id });
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-enrichment-factory", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var resolver = Resolver{ .root = root };
    var factory = OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "default-owner",
                .dense_embedder = deterministic.interface(),
            },
        },
        .owner_id = "factory-owner",
    }, resolver.iface());

    var executor = db_enrichment_executor.DbEnrichmentExecutor.init(
        std.testing.allocator,
        .{ .backend = .threaded },
        factory.iface(),
    );
    defer executor.deinit();

    const iface = executor.executor();
    try iface.startGroup(7001);
    try std.testing.expect(iface.isActive(7001));
    try iface.stopGroup(7001);
    try std.testing.expect(!iface.isActive(7001));
}

test "open db runtime factory overrides enrichment owner id when configured" {
    var factory = OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "default-owner",
            },
        },
        .owner_id = "node-2",
    }, undefined);

    const effective = factory.effectiveOpenOptions();
    try std.testing.expect(effective.enrichment != null);
    try std.testing.expectEqualStrings("node-2", effective.enrichment.?.owner_id);
}

test "open db runtime factory preserves lease fencing across owner takeover" {
    const embedder_mod = @import("../storage/db/enrichment/embedder.zig");
    const db_types = @import("../storage/db/types.zig");

    const Resolver = struct {
        root: []const u8,

        fn iface(self: *@This()) GroupDbPathResolver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .resolve_path = resolvePath,
                },
            };
        }

        fn resolvePath(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ self.root, group_id });
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-enrichment-owner-takeover", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var deterministic_a = embedder_mod.DeterministicDenseEmbedder{};
    var deterministic_b = embedder_mod.DeterministicDenseEmbedder{};
    var resolver = Resolver{ .root = root };

    var factory_a = OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-a",
                .lease_ttl_ms = 250,
                .dense_embedder = deterministic_a.interface(),
            },
        },
        .owner_id = "node-a",
    }, resolver.iface());
    var factory_b = OpenDbRuntimeFactory.init(std.testing.allocator, .{
        .open_options = .{
            .enrichment = .{
                .owner_id = "node-b",
                .lease_ttl_ms = 250,
                .dense_embedder = deterministic_b.interface(),
            },
        },
        .owner_id = "node-b",
    }, resolver.iface());

    var handle_a = try OpenDbRuntimeFactory.startRuntime(@ptrCast(&factory_a), 7002);
    defer handle_a.deinit(std.testing.allocator);
    const runtime_a: *DbRuntimeHandle = @ptrCast(@alignCast(handle_a.ptr));
    const db_a = runtime_a.db.?;

    try db_a.addIndex(.{
        .name = "dv_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"body_dense_v1\"}}",
    });

    try db_a.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"owner a text\"}" },
        },
        .sync_level = .full_index,
    });
    try db_a.enrichment_runtime.?.waitForApplied(1);

    const stats_a = try db_a.stats(std.testing.allocator);
    defer db_types.freeDBStats(std.testing.allocator, stats_a);
    try std.testing.expect(stats_a.enrichment.has_lease);
    try std.testing.expect(stats_a.enrichment.acquisition_count > 0);

    try handle_a.stop();
    var handle_b = try OpenDbRuntimeFactory.startRuntime(@ptrCast(&factory_b), 7002);
    defer handle_b.deinit(std.testing.allocator);
    const runtime_b: *DbRuntimeHandle = @ptrCast(@alignCast(handle_b.ptr));
    const db_b = runtime_b.db.?;

    try db_b.batch(.{
        .writes = &.{
            .{ .key = "doc:c", .value = "{\"body\":\"owner b reacquire text\"}" },
        },
        .sync_level = .full_index,
    });
    try db_b.enrichment_runtime.?.waitForApplied(2);
    const stats_b = try db_b.stats(std.testing.allocator);
    defer db_types.freeDBStats(std.testing.allocator, stats_b);
    try std.testing.expect(stats_b.enrichment.has_lease);
    try std.testing.expect(stats_b.enrichment.acquisition_count > 0);
    try std.testing.expect(stats_b.enrichment.applied_sequence >= 2);
}
