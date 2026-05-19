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
const Allocator = std.mem.Allocator;
const catalog_service = @import("../catalog/service.zig");

pub const PublishRunStats = struct {
    published_namespaces: usize = 0,
    head_conflicts: usize = 0,
    idle_namespaces: usize = 0,
};

pub const BackgroundPublisher = struct {
    alloc: Allocator,
    catalog: *catalog_service.CatalogService,
    poll_interval_ms: u64,
    thread: ?std.Thread = null,
    stop_requested: std.atomic.Value(bool) = .init(false),

    pub fn init(alloc: Allocator, catalog: *catalog_service.CatalogService, poll_interval_ms: u64) BackgroundPublisher {
        return .{
            .alloc = alloc,
            .catalog = catalog,
            .poll_interval_ms = poll_interval_ms,
        };
    }

    pub fn deinit(self: *BackgroundPublisher) void {
        self.stop();
        self.* = undefined;
    }

    pub fn start(self: *BackgroundPublisher) !void {
        if (self.thread != null) return error.AlreadyStarted;
        self.stop_requested.store(false, .monotonic);
        self.thread = try std.Thread.spawn(.{}, runLoop, .{self});
    }

    pub fn stop(self: *BackgroundPublisher) void {
        self.stop_requested.store(true, .monotonic);
        if (self.thread) |thread| {
            thread.join();
            self.thread = null;
        }
    }

    pub fn runOnce(self: *BackgroundPublisher) !PublishRunStats {
        const namespaces = try self.catalog.listNamespacesAlloc(self.alloc);
        defer self.catalog.freeNamespaces(self.alloc, namespaces);

        var stats = PublishRunStats{};
        for (namespaces) |namespace| {
            var status = self.catalog.buildStatus(namespace.name) catch |err| switch (err) {
                error.FileNotFound => {
                    stats.idle_namespaces += 1;
                    continue;
                },
                else => return err,
            };
            defer status.deinit(self.alloc);
            if (!status.publish_recommended) {
                stats.idle_namespaces += 1;
                continue;
            }

            var result = self.catalog.buildNamespace(namespace.name) catch |err| switch (err) {
                error.HeadChanged => {
                    stats.head_conflicts += 1;
                    continue;
                },
                error.FileNotFound => {
                    stats.idle_namespaces += 1;
                    continue;
                },
                else => return err,
            };
            defer result.deinit(self.alloc);
            if (result.published) {
                stats.published_namespaces += 1;
            } else {
                // Another publisher may win between buildStatus() and buildNamespace().
                // Treat the resulting no-op like an idle namespace rather than losing it
                // from the run accounting entirely.
                stats.idle_namespaces += 1;
            }
        }

        return stats;
    }

    fn runLoop(self: *BackgroundPublisher) void {
        while (!self.stop_requested.load(.monotonic)) {
            _ = self.runOnce() catch PublishRunStats{};
            sleepMs(@max(self.poll_interval_ms, 1));
        }
    }
};

test "background publisher runOnce publishes namespaces with pending WAL" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-run-once");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-run-once");
    const wal_root = tmpPath(&wal_root_buf, "wal-run-once");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-run-once");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_service.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    _ = try catalog.ensureNamespace("docs", 100);

    const mutation = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    var ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &mutation,
    });
    defer ingest.deinit(alloc);

    var publisher = BackgroundPublisher.init(alloc, &catalog, 1);
    const published = try publisher.runOnce();
    try std.testing.expectEqual(@as(usize, 1), published.published_namespaces);
    try std.testing.expectEqual(@as(usize, 0), published.head_conflicts);
    try std.testing.expectEqual(@as(u64, 1), try progress_store.getHead("docs"));
}

test "background publisher loop publishes asynchronously and latest reads remain valid" {
    const alloc = std.heap.page_allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-loop");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-loop");
    const wal_root = tmpPath(&wal_root_buf, "wal-loop");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-loop");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog = catalog_service.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder, &catalog_store);
    defer catalog.deinit();
    _ = try catalog.ensureNamespace("docs", 100);

    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder);
    const initial = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var first_ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &initial,
    });
    defer first_ingest.deinit(alloc);
    var first_build = try builder.publishNamespace("docs");
    defer first_build.deinit(alloc);

    const next = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-b", .body = "beta" },
    };
    var next_ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 300,
        .mutations = &next,
    });
    defer next_ingest.deinit(alloc);

    var publisher = BackgroundPublisher.init(alloc, &catalog, 1);
    defer publisher.deinit();
    try publisher.start();

    var query = @import("../query/mod.zig").QueryRuntime.init(alloc, &artifact_store, &manifest_store, &progress_store);
    defer query.deinit();
    var latest_seen_tail = false;
    var attempts: usize = 0;
    while (attempts < 50) : (attempts += 1) {
        const head = progress_store.getHead("docs") catch 0;
        var session = try query.openHeadSession("docs");
        defer session.deinit();
        const tail = try wal_store.readFromAlloc("docs", session.manifest.wal_end_lsn + 1);
        defer @import("../wal/mod.zig").freeRecords(alloc, tail);
        try std.testing.expect(session.manifest.wal_end_lsn <= try wal_store.latestLsn("docs"));
        if (tail.len > 0) latest_seen_tail = true;
        if (head >= 2) break;
        sleepMs(5);
    }

    try std.testing.expect(latest_seen_tail);
    try std.testing.expectEqual(@as(u64, 2), try progress_store.getHead("docs"));
}

test "concurrent background publishers yield a single publish winner" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    var catalog_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-race");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-race");
    const wal_root = tmpPath(&wal_root_buf, "wal-race");
    const catalog_root = tmpPath(&catalog_root_buf, "catalog-race");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);
    defer cleanupTmp(catalog_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try @import("../wal/mod.zig").FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var fs_catalog = try @import("../catalog/fs_store.zig").FsStore.init(alloc, std.mem.span(catalog_root));
    var catalog_store = fs_catalog.catalogStore();
    defer catalog_store.deinit();

    var builder_a = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var builder_b = @import("builder.zig").Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var catalog_a = catalog_service.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder_a, &catalog_store);
    defer catalog_a.deinit();
    var catalog_b = catalog_service.CatalogService.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store, &builder_b, &catalog_store);
    defer catalog_b.deinit();
    _ = try catalog_a.ensureNamespace("docs", 100);

    const mutation = [_]@import("../api/types.zig").DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var api = @import("../api/service.zig").Service.init(alloc, &wal_store, &builder_a);
    var ingest = try api.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 200,
        .mutations = &mutation,
    });
    defer ingest.deinit(alloc);

    const RaceState = struct {
        pub_a: BackgroundPublisher,
        pub_b: BackgroundPublisher,
        stats_a: PublishRunStats = .{},
        stats_b: PublishRunStats = .{},

        fn runA(self: *@This()) void {
            self.stats_a = self.pub_a.runOnce() catch PublishRunStats{};
        }

        fn runB(self: *@This()) void {
            self.stats_b = self.pub_b.runOnce() catch PublishRunStats{};
        }
    };

    var state = RaceState{
        .pub_a = BackgroundPublisher.init(alloc, &catalog_a, 1),
        .pub_b = BackgroundPublisher.init(alloc, &catalog_b, 1),
    };
    const thread_a = try std.Thread.spawn(.{}, RaceState.runA, .{&state});
    const thread_b = try std.Thread.spawn(.{}, RaceState.runB, .{&state});
    thread_a.join();
    thread_b.join();

    try std.testing.expectEqual(@as(usize, 1), state.stats_a.published_namespaces + state.stats_b.published_namespaces);
    try std.testing.expectEqual(@as(usize, 1), state.stats_a.head_conflicts + state.stats_b.head_conflicts + state.stats_a.idle_namespaces + state.stats_b.idle_namespaces);
    try std.testing.expectEqual(@as(u64, 1), try progress_store.getHead("docs"));
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-coordinator-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

fn sleepMs(ms: u64) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromMilliseconds(@intCast(if (ms == 0) @as(u64, 1) else ms)),
    }, io_impl.io()) catch {};
}
