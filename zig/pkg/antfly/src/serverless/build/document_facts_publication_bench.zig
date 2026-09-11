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

//! Opt-in end-to-end publication qualification. Artifact counters count calls
//! at the store boundary, not provider-internal requests. Filesystem manifest,
//! WAL, lease renewal and fenced HEAD latency are included in wall time.
const std = @import("std");
const a = std.heap.page_allocator;
const artifacts_mod = @import("../artifacts/mod.zig");
const store_mod = @import("../artifacts/store.zig");
const manifest_mod = @import("../manifest/mod.zig");
const catalog_mod = @import("../catalog/mod.zig");
const wal_mod = @import("../wal/mod.zig");
const builder_mod = @import("builder.zig");
const publication_plan = @import("publication_plan.zig");
const api_codec = @import("../api/codec.zig");
const Cancellation = @import("../../common/cancellation.zig").CancellationToken;
const document_facts = @import("document_facts.zig");
const page_tree = @import("../graph_segment/page_tree.zig");

test "serverless external metadata retention qualification benchmark" {
    if (std.c.getenv("ANTFLY_DOCUMENT_FACTS_BENCH") == null) return error.SkipZigTest;
    const metadata = @import("external_publication_metadata.zig");
    var runtime = std.Io.Threaded.init(a, .{});
    defer runtime.deinit();
    for ([_]u64{ 1024, 16384 }) |count| for ([_]bool{ false, true }) |rejected| {
        var source = try metadata.testing.fixtureAlloc(a, count);
        defer source.deinit(a);
        if (rejected) for (source.artifacts) |*ref| {
            if (ref.kind != .graph_metric_segment) continue;
            ref.materializer_fingerprint = @import("lake_graph_metric.zig").materializerFingerprint(.{});
            if (std.mem.eql(u8, ref.name, "9:graph_idx4:rank")) ref.graph_metric_materialization_state = .rejected;
        };
        var plan = publication_plan.TablePublicationPlan{ .targets = .{ .published_search_sources = .{} } };
        plan.table_definition = .{
            .schema_json = source.stats.schema_json,
            .read_schema_json = @constCast("{\"description\":\"metadata-only refresh\"}"),
            .indexes_json = source.stats.indexes_json,
        };
        plan.policy = source.stats.policy;
        var samples: [5]i96 = undefined;
        for (0..6) |round| {
            const start = std.Io.Timestamp.now(runtime.io(), .awake).toNanoseconds();
            var result = try metadata.reconcileAlloc(a, source, plan);
            defer result.deinit(a);
            const elapsed = std.Io.Timestamp.now(runtime.io(), .awake).toNanoseconds() - start;
            try std.testing.expectEqual(@as(usize, 5), result.artifacts.len);
            try std.testing.expectEqual(count, result.stats.document_count);
            if (round != 0) samples[round - 1] = elapsed;
        }
        std.mem.sort(i96, &samples, {}, std.sort.asc(i96));
        std.debug.print("external_metadata_retention docs={} rejected={} retained_sidecars=5 median_ns={} artifact_io_capability=false samples=5\n", .{ count, rejected, samples[2] });
    };
}

test "serverless pending work index qualification benchmark" {
    if (std.c.getenv("ANTFLY_DOCUMENT_FACTS_BENCH") == null) return error.SkipZigTest;
    var runtime = std.Io.Threaded.init(a, .{});
    defer runtime.deinit();
    for ([_]usize{ 1024, 16384 }) |count| try pendingWorkBenchmark(runtime.io(), count);
}

test "serverless pending cycle boundary qualification benchmark" {
    if (std.c.getenv("ANTFLY_DOCUMENT_FACTS_BENCH") == null) return error.SkipZigTest;
    var runtime = std.Io.Threaded.init(a, .{});
    defer runtime.deinit();
    for ([_]usize{ 1024, 16384 }) |count| {
        try pendingCycleBoundaryBenchmark(runtime.io(), count, 4);
        try pendingCycleBoundaryBenchmark(runtime.io(), count, count);
    }
}

fn pendingCycleBoundaryBenchmark(io: std.Io, count: usize, pending_count: usize) !void {
    var memory = page_tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    const ids = try a.alloc([16]u8, count);
    defer a.free(ids);
    const replacements = try a.alloc(document_facts.Replacement, count);
    defer a.free(replacements);
    for (replacements, 0..) |*replacement, i| {
        const id = try std.fmt.bufPrint(&ids[i], "doc-{d:0>12}", .{i});
        replacement.* = .{ .id = id, .value = .{
            .body = .{ .digest = @splat(1), .attempt = @splat(1), .bytes = 1024 },
            .last_lsn = 1,
            .last_timestamp_ns = 1,
            .pending = if (i >= count - pending_count) 1 else 0,
        } };
    }
    const empty = document_facts.Root{ .domain = store.domain, .policy_fingerprint = @splat(1) };
    var initial = try document_facts.planAlloc(a, store, empty, replacements, 1);
    defer initial.deinit();
    const root = try initial.publish(store, empty);
    const Sample = struct { scan_ns: i96, seek_ns: i96, scan_reads: usize, seek_reads: usize };
    var samples: [5]Sample = undefined;
    for (0..6) |round| {
        memory.reads = 0;
        const scan_start = std.Io.Timestamp.now(io, .awake).toNanoseconds();
        var scan = try document_facts.pendingCursor(a, store, root, 0, "");
        defer scan.deinit();
        var visited: usize = 0;
        while (try scan.next()) |_| visited += 1;
        const scan_ns = std.Io.Timestamp.now(io, .awake).toNanoseconds() - scan_start;
        const scan_reads = memory.reads;
        try std.testing.expectEqual(pending_count, visited);

        // Capture an owned cycle boundary by authenticated subtree rank, not
        // by traversing the pending set. This cost occurs once per cycle;
        // resumed passes use the durable key without another tail lookup.
        memory.reads = 0;
        const seek_start = std.Io.Timestamp.now(io, .awake).toNanoseconds();
        var tail = try document_facts.pendingCursorAtRank(a, store, root, 0, root.counts[3] - 1);
        defer tail.deinit();
        const last = (try tail.next()).?;
        const upper = try a.dupe(u8, last.order_key);
        defer a.free(upper);
        const seek_ns = std.Io.Timestamp.now(io, .awake).toNanoseconds() - seek_start;
        try std.testing.expectEqualStrings(&ids[count - 1], upper[8..]);
        try std.testing.expect(memory.reads <= root.pending_pages[0].?.height + 1);
        if (round != 0) samples[round - 1] = .{ .scan_ns = scan_ns, .seek_ns = seek_ns, .scan_reads = scan_reads, .seek_reads = memory.reads };
    }
    std.mem.sort(Sample, &samples, {}, struct {
        fn less(_: void, lhs: Sample, rhs: Sample) bool {
            return lhs.seek_ns < rhs.seek_ns;
        }
    }.less);
    const median = samples[2];
    std.debug.print("pending_cycle_boundary docs={} pending={} scan_ns={} boundary_seek_ns={} scan_page_reads={} boundary_page_reads={} samples=5\n", .{ count, pending_count, median.scan_ns, median.seek_ns, median.scan_reads, median.seek_reads });
}

fn pendingWorkBenchmark(io: std.Io, count: usize) !void {
    var memory = page_tree.testing.MemoryStore{ .alloc = a };
    defer memory.deinit();
    const store = memory.store();
    const ids = try a.alloc([16]u8, count);
    defer a.free(ids);
    const replacements = try a.alloc(document_facts.Replacement, count);
    defer a.free(replacements);
    const complete = document_facts.Fact{
        .body = .{ .digest = @splat(1), .attempt = @splat(1), .bytes = 128 * 1024 * 1024 },
        .last_lsn = 1,
        .last_timestamp_ns = 1,
    };
    for (replacements, 0..) |*replacement, i| {
        const id = try std.fmt.bufPrint(&ids[i], "doc-{d:0>12}", .{i});
        var fact = complete;
        if (i >= count - 4) fact.pending = 1;
        replacement.* = .{ .id = id, .value = fact };
    }
    const empty = document_facts.Root{ .domain = store.domain, .policy_fingerprint = @splat(1) };
    var initial = try document_facts.planAlloc(a, store, empty, replacements, 1);
    defer initial.deinit();
    var root = try initial.publish(store, empty);
    const Sample = struct { full_ns: i96, pending_ns: i96, full_reads: usize, pending_reads: usize };
    var samples: [5]Sample = undefined;
    for (0..6) |round| {
        // A new unrelated publication must not make the completed prefix part
        // of the worker's scan again. Body blobs deliberately do not exist:
        // this measures routing work, not body cache or transport throughput.
        var changed = complete;
        changed.last_lsn = round + 2;
        var update = try document_facts.planAlloc(a, store, root, &.{.{ .id = &ids[0], .value = changed }}, round + 2);
        defer update.deinit();
        const next = try update.publish(store, root);
        try std.testing.expectEqualDeep(root.pending_pages, next.pending_pages);
        root = next;
        memory.reads = 0;
        const full_start = std.Io.Timestamp.now(io, .awake).toNanoseconds();
        var full = try page_tree.Cursor.init(a, store, root.page, "", null);
        defer full.deinit();
        var full_pending: usize = 0;
        while (try full.next()) |record| {
            if ((try document_facts.Fact.decode(record.value)).pending & 1 != 0) full_pending += 1;
        }
        const full_ns = std.Io.Timestamp.now(io, .awake).toNanoseconds() - full_start;
        const full_reads = memory.reads;
        memory.reads = 0;
        const pending_start = std.Io.Timestamp.now(io, .awake).toNanoseconds();
        var pending = try document_facts.pendingCursor(a, store, root, 0, "");
        defer pending.deinit();
        var found: usize = 0;
        while (try pending.next()) |_| found += 1;
        const pending_ns = std.Io.Timestamp.now(io, .awake).toNanoseconds() - pending_start;
        try std.testing.expectEqual(@as(usize, 4), found);
        try std.testing.expectEqual(full_pending, found);
        try std.testing.expectEqual(@as(usize, 1), memory.reads);
        if (round != 0) samples[round - 1] = .{ .full_ns = full_ns, .pending_ns = pending_ns, .full_reads = full_reads, .pending_reads = memory.reads };
    }
    std.mem.sort(Sample, &samples, {}, struct {
        fn less(_: void, lhs: Sample, rhs: Sample) bool {
            return lhs.full_ns < rhs.full_ns;
        }
    }.less);
    const median = samples[2];
    std.debug.print("pending_work_qualification docs={} pending=4 full_scan_ns={} indexed_scan_ns={} full_page_reads={} indexed_page_reads={} samples=5\n", .{ count, median.full_ns, median.pending_ns, median.full_reads, median.pending_reads });
}

const Counts = struct { gets: u64 = 0, read_bytes: u64 = 0, puts: u64 = 0, write_bytes: u64 = 0, stats: u64 = 0, verifies: u64 = 0 };
const CountingStore = struct {
    inner: *store_mod.ArtifactStore,
    counts: Counts = .{},
    reject_document_bodies: bool = false,

    fn capability(self: *@This()) store_mod.ArtifactStore {
        return .{ .allocator = self.inner.allocator, .ptr = self, .vtable = &.{
            .deinit = deinit,
            .put = put,
            .put_with_cancellation = putUntil,
            .put_scoped = putScoped,
            .get_alloc = get,
            .get_alloc_with_cancellation = getUntil,
            .get_range_alloc = range,
            .get_range_alloc_with_cancellation = rangeUntil,
            .get_verified_range_alloc_with_cancellation = verifiedRange,
            .get_verified_range_alloc_with_budget = verifiedRangeBudget,
            .stat = stat,
            .stat_with_cancellation = statUntil,
            .verify_content = verify,
            .delete = delete,
        } };
    }
    fn selfFrom(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }
    fn deinit(_: std.mem.Allocator, _: *anyopaque) void {}
    fn put(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8) !store_mod.ArtifactMetadata {
        return putUntil(ptr, alloc, bytes, .none);
    }
    fn putUntil(ptr: *anyopaque, alloc: std.mem.Allocator, bytes: []const u8, cancel: Cancellation) !store_mod.ArtifactMetadata {
        const self = selfFrom(ptr);
        self.counts.puts += 1;
        self.counts.write_bytes += bytes.len;
        var inner = self.inner.*;
        inner.allocator = alloc;
        return inner.putWithCancellation(bytes, cancel);
    }
    fn putScoped(ptr: *anyopaque, alloc: std.mem.Allocator, scope: store_mod.UploadScope, bytes: []const u8, cancel: Cancellation) !store_mod.ArtifactMetadata {
        const self = selfFrom(ptr);
        self.counts.puts += 1;
        self.counts.write_bytes += bytes.len;
        var inner = self.inner.*;
        inner.allocator = alloc;
        return inner.putScoped(scope, bytes, cancel);
    }
    fn get(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8) ![]u8 {
        return getUntil(ptr, alloc, id, .none);
    }
    fn getUntil(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, cancel: Cancellation) ![]u8 {
        const self = selfFrom(ptr);
        const bytes = try self.inner.getAllocWithCancellationUsingAllocator(alloc, id, cancel);
        errdefer alloc.free(bytes);
        if (self.reject_document_bodies and std.mem.startsWith(u8, bytes, "AFDBODY1")) return error.UnexpectedDocumentBodyRead;
        self.counts.gets += 1;
        self.counts.read_bytes += bytes.len;
        return bytes;
    }
    fn range(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, offset: u64, len: usize) ![]u8 {
        return rangeUntil(ptr, alloc, id, offset, len, .none);
    }
    fn rangeUntil(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, offset: u64, len: usize, cancel: Cancellation) ![]u8 {
        const self = selfFrom(ptr);
        const bytes = try self.inner.getRangeAllocWithCancellationUsingAllocator(alloc, id, offset, len, cancel);
        errdefer alloc.free(bytes);
        if (self.reject_document_bodies and std.mem.startsWith(u8, bytes, "AFDBODY1")) return error.UnexpectedDocumentBodyRead;
        self.counts.gets += 1;
        self.counts.read_bytes += bytes.len;
        return bytes;
    }
    fn verifiedRange(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, size: u64, checksum: []const u8, offset: u64, len: usize, cancel: Cancellation) ![]u8 {
        const self = selfFrom(ptr);
        const bytes = try self.inner.getVerifiedRangeAllocWithCancellationUsingAllocator(alloc, id, size, checksum, offset, len, cancel);
        errdefer alloc.free(bytes);
        if (self.reject_document_bodies and std.mem.startsWith(u8, bytes, "AFDBODY1")) return error.UnexpectedDocumentBodyRead;
        self.counts.gets += 1;
        self.counts.read_bytes += bytes.len;
        return bytes;
    }
    fn verifiedRangeBudget(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, size: u64, checksum: []const u8, offset: u64, len: usize, cancel: Cancellation, remaining: *u64) ![]u8 {
        const self = selfFrom(ptr);
        const bytes = try self.inner.getVerifiedRangeAllocWithBudget(alloc, id, size, checksum, offset, len, cancel, remaining);
        errdefer alloc.free(bytes);
        if (self.reject_document_bodies and std.mem.startsWith(u8, bytes, "AFDBODY1")) return error.UnexpectedDocumentBodyRead;
        self.counts.gets += 1;
        self.counts.read_bytes += bytes.len;
        return bytes;
    }
    fn stat(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8) !store_mod.ArtifactMetadata {
        return statUntil(ptr, alloc, id, .none);
    }
    fn statUntil(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, cancel: Cancellation) !store_mod.ArtifactMetadata {
        const self = selfFrom(ptr);
        self.counts.stats += 1;
        return self.inner.statWithCancellationUsingAllocator(alloc, id, cancel);
    }
    fn verify(ptr: *anyopaque, alloc: std.mem.Allocator, id: []const u8, size: u64, checksum: []const u8, cancel: Cancellation) !void {
        const self = selfFrom(ptr);
        self.counts.verifies += 1;
        return self.inner.verifyContentWithCancellationUsingAllocator(alloc, id, size, checksum, cancel);
    }
    fn delete(ptr: *anyopaque, id: []const u8) !void {
        return selfFrom(ptr).inner.delete(id);
    }
};

test "serverless publication qualification benchmark" {
    if (std.c.getenv("ANTFLY_DOCUMENT_FACTS_BENCH") == null) return error.SkipZigTest;
    var runtime = std.Io.Threaded.init(a, .{});
    defer runtime.deinit();
    const io = runtime.io();
    const selected_count = if (std.c.getenv("ANTFLY_DOCUMENT_FACTS_BENCH_DOCS")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 0;
    const selected_degree = if (std.c.getenv("ANTFLY_DOCUMENT_FACTS_BENCH_DEGREE")) |raw| try std.fmt.parseInt(usize, std.mem.span(raw), 10) else 0;
    for ([_]usize{ 1024, 16384 }) |count| {
        if (selected_count != 0 and selected_count != count) continue;
        for ([_]usize{ 1, 1023 }) |degree| {
            if (selected_degree != 0 and selected_degree != degree) continue;
            try run(io, count, degree, false);
        }
    }
}

pub fn metadataRepublishRegression() !void {
    var runtime = std.Io.Threaded.init(a, .{});
    defer runtime.deinit();
    try run(runtime.io(), 16, 1, true);
}

fn run(io: std.Io, count: usize, degree: usize, metadata_only: bool) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/publication", .{tmp.sub_path});
    defer a.free(root);
    const artifact_path = try std.fs.path.join(a, &.{ root, "artifacts" });
    defer a.free(artifact_path);
    const manifest_path = try std.fs.path.join(a, &.{ root, "manifests" });
    defer a.free(manifest_path);
    const wal_path = try std.fs.path.join(a, &.{ root, "wal" });
    defer a.free(wal_path);
    var fs_artifacts = try artifacts_mod.FsStore.init(a, artifact_path);
    var underlying = fs_artifacts.artifactStore();
    defer underlying.deinit();
    var counting = CountingStore{ .inner = &underlying };
    var artifacts = counting.capability();
    var fs_manifests = try manifest_mod.FsStore.init(a, manifest_path);
    var manifests = fs_manifests.manifestStore();
    defer manifests.deinit();
    var fs_progress = try catalog_mod.FsProgressStore.init(a, manifest_path);
    var progress = fs_progress.progressStore();
    defer progress.deinit();
    var fs_wal = try wal_mod.FsStore.init(a, wal_path);
    var wal = fs_wal.walStore();
    defer wal.deinit();
    var builder = builder_mod.Builder.init(a, &artifacts, &manifests, &progress, &wal);
    builder.setIo(io);
    var plan = publication_plan.TablePublicationPlan{
        .targets = .{ .published_search_sources = .{ .items = &.{} }, .include_graph = true },
        .table_definition = .{ .indexes_json = @constCast("{\"graph_idx\":{\"type\":\"graph\",\"metrics\":{\"degree\":{\"kind\":\"degree\"},\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":20}}}}") },
        .artifact_actions = .{ .full_text = .drop, .dense_vector = .drop, .sparse_vector = .drop },
        .derived_output_actions = .{ .chunk_preview = .drop, .chunk_embeddings = .drop, .rerank_terms = .drop },
    };
    if (metadata_only) {
        plan.targets.published_search_sources = @import("../search_sources.zig").defaultPublishedSearchSources();
        plan.artifact_actions = .{};
    }
    for (0..count) |i| {
        const id = try std.fmt.allocPrint(a, "doc-{d:0>8}", .{i});
        defer a.free(id);
        const body = if (i == 0) try hubBody(degree, 0) else try a.dupe(u8, if (metadata_only)
            "{\"text\":\"alpha\",\"embedding\":[1,0],\"sparse_embedding\":{\"alpha\":1.0}}"
        else
            "{}");
        defer a.free(body);
        const payload = try api_codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = id, .body = body });
        defer a.free(payload);
        _ = try wal.append("docs", i + 1, payload);
    }
    var bootstrap = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
    bootstrap.deinit(a);
    try metadataRounds(io, &builder, &counting, &manifests, &progress, plan, count, degree, !metadata_only);
    if (metadata_only) return;
    const Sample = struct { ns: u64, predict_ns: u64, counts: Counts };
    var samples: [5]Sample = undefined;
    for (0..6) |round| {
        const body = try hubBody(degree, round + 1);
        defer a.free(body);
        counting.counts = .{};
        const start = std.Io.Timestamp.now(io, .awake);
        const payload = try api_codec.encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = "doc-00000000", .body = body });
        defer a.free(payload);
        _ = try wal.append("docs", count + round + 1, payload);
        const prediction_start = std.Io.Timestamp.now(io, .awake);
        if (try builder.predictPendingWalPublicationActionsAlloc("docs", .cosine, plan)) |value| {
            var prediction = value;
            prediction.deinit(a);
        }
        const prediction_end = std.Io.Timestamp.now(io, .awake);
        var result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, plan);
        defer result.deinit(a);
        try std.testing.expect(result.published);
        try std.testing.expectEqual(result.version, try progress.getHead("docs"));
        const end = std.Io.Timestamp.now(io, .awake);
        if (round != 0) samples[round - 1] = .{
            .ns = @intCast(end.toNanoseconds() - start.toNanoseconds()),
            .predict_ns = @intCast(prediction_end.toNanoseconds() - prediction_start.toNanoseconds()),
            .counts = counting.counts,
        };
    }
    std.mem.sort(Sample, &samples, {}, struct {
        fn less(_: void, lhs: Sample, rhs: Sample) bool {
            return lhs.ns < rhs.ns;
        }
    }.less);
    const median = samples[2];
    std.debug.print("publication_qualification docs={} degree={} median_ns={} prediction_ns={} artifact_gets={} read_bytes={} artifact_puts={} write_bytes={} stats={} verifies={} samples=5\n", .{
        count,              degree,                    median.ns,           median.predict_ns,      median.counts.gets, median.counts.read_bytes,
        median.counts.puts, median.counts.write_bytes, median.counts.stats, median.counts.verifies,
    });
}

fn metadataRounds(io: std.Io, builder: *builder_mod.Builder, counting: *CountingStore, manifests: *manifest_mod.ManifestStore, progress: *catalog_mod.ProgressStore, plan: publication_plan.TablePublicationPlan, count: usize, degree: usize, report: bool) !void {
    var source = try manifests.getAlloc("docs", try progress.getHead("docs"));
    defer source.deinit(a);
    const source_facts = factsRef(source);
    const Sample = struct { ns: u64, counts: Counts };
    var samples: [5]Sample = undefined;
    counting.reject_document_bodies = true;
    defer counting.reject_document_bodies = false;
    for (0..6) |round| {
        var metadata_plan = plan;
        metadata_plan.metadata_republish.index_definitions_changed = true;
        metadata_plan.artifact_actions = .{
            .document_segment = .reuse,
            .full_text = .reuse,
            .dense_vector = if (plan.artifact_actions.dense_vector == .drop) .drop else .reuse,
            .sparse_vector = if (plan.artifact_actions.sparse_vector == .drop) .drop else .reuse,
            .graph = .reuse,
        };
        metadata_plan.derived_output_actions = .{ .chunk_preview = .recompute, .chunk_embeddings = .recompute, .rerank_terms = .recompute };
        const indexes = try std.fmt.allocPrint(
            a,
            "{{\"alias_{d}\":{{\"type\":\"graph\",\"metrics\":{{\"degree\":{{\"kind\":\"degree\"}},\"rank\":{{\"kind\":\"pagerank\",\"max_iterations\":20}}}}}}}}",
            .{round},
        );
        defer a.free(indexes);
        metadata_plan.table_definition.indexes_json = indexes;
        counting.counts = .{};
        const start = std.Io.Timestamp.now(io, .awake);
        var result = try builder.publishNamespaceWithMetricAndPlan("docs", .cosine, metadata_plan);
        defer result.deinit(a);
        const elapsed: u64 = @intCast(std.Io.Timestamp.now(io, .awake).toNanoseconds() - start.toNanoseconds());
        try std.testing.expect(result.published);
        var published = try manifests.getAlloc("docs", result.version);
        defer published.deinit(a);
        try std.testing.expectEqualStrings(source_facts.artifact_id, factsRef(published).artifact_id);
        try std.testing.expectEqual(source.stats.document_count, published.stats.document_count);
        for (published.artifacts) |ref| {
            if (ref.kind != .graph_segment) continue;
            for (source.artifacts) |prior| {
                if (prior.kind != .graph_segment) continue;
                try std.testing.expectEqualStrings(prior.artifact_id, ref.artifact_id);
                try std.testing.expectEqual(prior.edge_generation, ref.edge_generation);
                break;
            }
        }
        try std.testing.expectEqual(@as(u64, 0), counting.counts.puts);
        try std.testing.expect(counting.counts.read_bytes < 16 * 1024);
        if (round != 0) samples[round - 1] = .{ .ns = elapsed, .counts = counting.counts };
    }
    std.mem.sort(Sample, &samples, {}, struct {
        fn less(_: void, lhs: Sample, rhs: Sample) bool {
            return lhs.ns < rhs.ns;
        }
    }.less);
    const median = samples[2];
    if (report) std.debug.print("metadata_publication_qualification docs={} degree={} median_ns={} artifact_gets={} read_bytes={} artifact_puts={} write_bytes={} stats={} verifies={} samples=5\n", .{
        count, degree, median.ns, median.counts.gets, median.counts.read_bytes, median.counts.puts, median.counts.write_bytes, median.counts.stats, median.counts.verifies,
    });
}

fn factsRef(manifest: manifest_mod.Manifest) manifest_mod.ArtifactRef {
    for (manifest.artifacts) |ref| if (ref.kind == .document_facts) return ref;
    unreachable;
}

fn hubBody(degree: usize, round: usize) ![]u8 {
    const Edge = struct { target: []const u8, edge_type: []const u8 = "links", weight: f32 };
    const edges = try a.alloc(Edge, degree);
    defer a.free(edges);
    var initialized: usize = 0;
    defer for (edges[0..initialized]) |edge| a.free(edge.target);
    for (edges, 0..) |*edge, i| {
        edge.* = .{ .target = try std.fmt.allocPrint(a, "doc-{d:0>8}", .{i + 1}), .weight = if (i == 0) @as(f32, @floatFromInt(1 + round % 2)) else 1 };
        initialized += 1;
    }
    return std.json.Stringify.valueAlloc(a, .{ .text = "", .graph_edges = edges }, .{});
}
