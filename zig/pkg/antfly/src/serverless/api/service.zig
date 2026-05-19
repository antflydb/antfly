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
const api_types = @import("types.zig");
const api_codec = @import("codec.zig");
const builder_mod = @import("../build/builder.zig");
const wal_mod = @import("../wal/mod.zig");

pub const Service = struct {
    alloc: Allocator,
    wal: *wal_mod.WalStore,
    builder: *builder_mod.Builder,

    pub fn init(alloc: Allocator, wal: *wal_mod.WalStore, builder: *builder_mod.Builder) Service {
        return .{
            .alloc = alloc,
            .wal = wal,
            .builder = builder,
        };
    }

    pub fn ingestBatch(self: *Service, req: api_types.IngestBatchRequest) !api_types.IngestBatchResult {
        if (req.mutations.len == 0) return .{
            .namespace = try self.alloc.dupe(u8, req.namespace),
            .mutation_count = 0,
            .start_lsn = 0,
            .end_lsn = 0,
        };

        var start_lsn: u64 = 0;
        var end_lsn: u64 = 0;
        for (req.mutations, 0..) |mutation, idx| {
            const encoded = try api_codec.encodeMutationAlloc(self.alloc, mutation);
            defer self.alloc.free(encoded);
            const lsn = try self.wal.append(req.namespace, req.timestamp_ns, encoded);
            if (idx == 0) start_lsn = lsn;
            end_lsn = lsn;
        }

        return .{
            .namespace = try self.alloc.dupe(u8, req.namespace),
            .mutation_count = req.mutations.len,
            .start_lsn = start_lsn,
            .end_lsn = end_lsn,
        };
    }

    pub fn ingestTableBatch(self: *Service, req: api_types.TableIngestBatchRequest) !api_types.TableIngestBatchResult {
        var result = try self.ingestBatch(.{
            .namespace = req.table_name,
            .timestamp_ns = req.timestamp_ns,
            .mutations = req.mutations,
        });
        defer result.deinit(self.alloc);

        return .{
            .table_name = try self.alloc.dupe(u8, req.table_name),
            .mutation_count = result.mutation_count,
            .start_lsn = result.start_lsn,
            .end_lsn = result.end_lsn,
        };
    }

    pub fn buildNamespace(self: *Service, namespace: []const u8) !builder_mod.BuildResult {
        return try self.builder.publishNamespace(namespace);
    }

    pub fn buildTable(self: *Service, table_name: []const u8) !api_types.TableBuildResult {
        var result = try self.buildNamespace(table_name);
        defer result.deinit(self.alloc);

        return .{
            .table_name = try self.alloc.dupe(u8, table_name),
            .published = result.published,
            .version = result.version,
            .wal_start_lsn = result.wal_start_lsn,
            .wal_end_lsn = result.wal_end_lsn,
            .artifact_count = result.artifact_count,
        };
    }
};

test "api service ingests typed mutations and builds published namespace" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests");
    const wal_root = tmpPath(&wal_root_buf, "wal");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var service = Service.init(alloc, &wal_store, &builder);

    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
        .{ .kind = .delete, .doc_id = "doc-b", .body = null },
    };
    var ingest = try service.ingestBatch(.{
        .namespace = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), ingest.mutation_count);
    try std.testing.expectEqual(@as(u64, 1), ingest.start_lsn);
    try std.testing.expectEqual(@as(u64, 2), ingest.end_lsn);

    var build = try service.buildNamespace("docs");
    defer build.deinit(alloc);
    try std.testing.expect(build.published);
    try std.testing.expectEqual(@as(u64, 1), build.version);
}

test "api service exposes the table public API over namespace-backed ingest and build" {
    const alloc = std.testing.allocator;

    var artifact_root_buf: [256]u8 = undefined;
    var manifest_root_buf: [256]u8 = undefined;
    var wal_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-table");
    const manifest_root = tmpPath(&manifest_root_buf, "manifests-table");
    const wal_root = tmpPath(&wal_root_buf, "wal-table");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(manifest_root);
    defer cleanupTmp(wal_root);

    var fs_artifacts = try @import("../artifacts/mod.zig").FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var fs_manifests = try @import("../manifest/mod.zig").FsStore.init(alloc, std.mem.span(manifest_root));
    var manifest_store = fs_manifests.manifestStore();
    defer manifest_store.deinit();

    var fs_progress = try @import("../catalog/fs_progress_store.zig").FsProgressStore.init(alloc, std.mem.span(manifest_root));
    var progress_store = fs_progress.progressStore();
    defer progress_store.deinit();

    var fs_wal = try wal_mod.FsStore.init(alloc, std.mem.span(wal_root));
    var wal_store = fs_wal.walStore();
    defer wal_store.deinit();

    var builder = builder_mod.Builder.init(alloc, &artifact_store, &manifest_store, &progress_store, &wal_store);
    var service = Service.init(alloc, &wal_store, &builder);

    const mutations = [_]api_types.DocumentMutation{
        .{ .kind = .upsert, .doc_id = "doc-a", .body = "alpha" },
    };
    var ingest = try service.ingestTableBatch(.{
        .table_name = "docs",
        .timestamp_ns = 123,
        .mutations = &mutations,
    });
    defer ingest.deinit(alloc);

    try std.testing.expectEqualStrings("docs", ingest.table_name);
    try std.testing.expectEqual(@as(usize, 1), ingest.mutation_count);

    var build = try service.buildTable("docs");
    defer build.deinit(alloc);
    try std.testing.expectEqualStrings("docs", build.table_name);
    try std.testing.expect(build.published);
    try std.testing.expectEqual(@as(u64, 1), build.version);
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-api-{s}-{d}-{d}\x00", .{
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
