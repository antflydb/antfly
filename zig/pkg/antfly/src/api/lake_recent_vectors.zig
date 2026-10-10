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

//! Durable recent native vector generations, built independently of Parquet
//! publication. Jobs are keyed by exact archive/recipe/WAL cut; stale completion
//! can only populate its own cut and cannot advance another cut's coverage.
const std = @import("std");
const local = @import("antfly_local_sources");
const stores = @import("../serverless/artifacts/store.zig");
const Store = @import("lake_index_store.zig").Store;
const overlay_api = @import("lake_search_overlay.zig");
const dense = @import("lake_index_native_dense.zig");
const sparse = @import("lake_index_native_sparse.zig");
const files = @import("lake_index_native_files.zig");
const A = std.mem.Allocator;
const V = std.json.Value;
const Declared = local.serverless_segment_sidecar_manifest.DeclaredArtifact;
const Context = local.serverless_query_lake_read_context.Context;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const retention_ms: u64 = 24 * 60 * 60 * 1000;
pub const Job = struct {
    version: u16 = 1,
    state: enum { building, ready, failed } = .building,
    table_id: u64,
    object_generation: u64,
    wal_lsn: u64,
    archive_generation: u64,
    desired: [32]u8,
    cut: [32]u8,
    lease_until_ms: u64 = 0,
    expires_ms: u64 = 0,
    attempts: u32 = 0,
    owner_token: [16]u8 = @splat(0),
    next_retry_ms: u64 = 0,
    last_error: ?[]const u8 = null,
    declarations: []const Declared = &.{},
};
fn scopeDomain(table: u64, identity: [32]u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-lake-recent-vector-generations-v1");
    hash.update(&identity);
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, table, .little);
    hash.update(&bytes);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}
fn cutDigest(a: A, table: local.common_topology_records.TableRecord, publication: local.metadata_lake_index_catalog.Publication, overlay: *const overlay_api.Overlay) ![32]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .table = table.table_id, .generation = table.object_storage_generation, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table), .publication = publication.generation, .source = publication.signature.source, .pending = overlay.pending }, .{});
    defer a.free(bytes);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &digest, .{});
    return digest;
}
fn jobKey(a: A, store: *Store, table: u64, cut: [32]u8) ![]u8 {
    return std.fmt.allocPrint(a, "{s}{s}recent-search/{d}/{s}.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", table, std.fmt.bytesToHex(cut, .lower) });
}
fn cancellation(context: Context) Cancellation {
    return if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else .none;
}
pub fn prepare(a: A, table: local.common_topology_records.TableRecord, publication: local.metadata_lake_index_catalog.Publication, overlay: *const overlay_api.Overlay, declarations: []const Declared, store: *Store, context: Context, options: local.inference_managed_embedder.InitOptions, build: bool, cut_expires_ms: u64) ![]const Declared {
    try context.ensureActive();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const cut = try cutDigest(scratch, table, publication, overlay);
    const key = try jobKey(scratch, store, table.table_id, cut);
    var client = store.opened.client;
    var saved = client.getObject(store.opened.bucket, key, .{ .max_response_bytes = 2 * 1024 * 1024, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else null }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (saved) |*value| value.deinit(client.allocator);
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    var job: Job = if (saved) |value| try std.json.parseFromSliceLeaky(Job, scratch, value.body, .{ .allocate = .alloc_always }) else .{ .table_id = table.table_id, .object_generation = table.object_storage_generation, .wal_lsn = overlay.pending.lsn, .archive_generation = publication.generation, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table), .cut = cut };
    if (job.version != 1 or !std.mem.eql(u8, &job.cut, &cut) or job.table_id != table.table_id or job.object_generation != table.object_storage_generation or job.wal_lsn != overlay.pending.lsn or job.archive_generation != publication.generation or !std.mem.eql(u8, &job.desired, &local.metadata_lake_index_catalog.desiredFingerprint(table))) return error.CatalogGenerationChanged;
    if (job.state == .ready and job.expires_ms > @max(now, cut_expires_ms)) {
        const encoded = try std.json.Stringify.valueAlloc(scratch, job.declarations, .{});
        return std.json.parseFromSliceLeaky([]const Declared, a, encoded, .{ .allocate = .alloc_always });
    }
    if (!build) return error.IndexRebuilding;
    if ((job.state == .building and job.lease_until_ms > now) or (job.state == .failed and job.next_retry_ms > now)) return error.IndexRebuilding;
    const io = context.io orelse return error.UnsupportedQueryRequest;
    job.state = .building;
    job.lease_until_ms = now +| 120_000;
    job.expires_ms = @max(now +| retention_ms, cut_expires_ms +| 1);
    job.attempts +|= 1;
    io.random(&job.owner_token);
    job.last_error = null;
    var claimed = client.putObject(store.opened.bucket, key, try std.json.Stringify.valueAlloc(scratch, job, .{}), .{ .if_none_match = saved == null, .if_match_etag = if (saved) |value| value.metadata.etag orelse return error.MissingObjectEtag else null, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else null }) catch |err| switch (err) {
        error.PreconditionFailed, error.ObjectAlreadyExists => return error.IndexRebuilding,
        else => {
            // A canceled/lost response may follow a successful claim write.
            // Recover only our random ownership token within the same bounded
            // cleanup path; never release a concurrent or successor claim.
            releaseClaim(scratch, store, key, "", job, io) catch {};
            return err;
        },
    };
    defer claimed.deinit(client.allocator);
    var job_finished = false;
    // Shutdown cancellation must not strand this claim until lease expiry.
    // A separate one-second cleanup budget releases only our ETag; a crashed
    // process still relies on normal lease expiry, and a newer owner is fenced.
    errdefer if (!job_finished) releaseClaim(scratch, store, key, claimed.etag orelse "", job, io) catch {};
    var artifacts = store.artifactStore();
    artifacts.upload_scope = try stores.UploadScope.forPublication(scopeDomain(table.table_id, store.identity), job.expires_ms, io);
    const output: ?[]const Declared = buildSegments(a, scratch, table, overlay, declarations, &artifacts, store, context, options) catch |err| failed: {
        job.state = .failed;
        job.last_error = @errorName(err);
        job.next_retry_ms = now +| @min(@as(u64, 300_000), @as(u64, job.attempts) * 5_000);
        job.declarations = &.{};
        break :failed @as(?[]const Declared, null);
    };
    if (output) |ready| {
        job.state = .ready;
        job.declarations = ready;
        job.last_error = null;
    }
    // CAS ties completion to this claim; an expired worker cannot overwrite
    // a replacement. Immutable artifacts are safe orphan candidates on failure.
    var completed = try client.putObject(store.opened.bucket, key, try std.json.Stringify.valueAlloc(scratch, job, .{}), .{ .if_match_etag = claimed.etag orelse return error.MissingObjectEtag, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else null });
    job_finished = true;
    completed.deinit(client.allocator);
    const status_key = try std.fmt.allocPrint(scratch, "{s}{s}recent-search/{d}/status.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", table.table_id });
    // Status is an observation, not commit authority. Fence it with both the
    // WAL position and an ETag so a delayed older completion cannot regress it.
    var prior_status = client.getObject(store.opened.bucket, status_key, .{ .max_response_bytes = 2 * 1024 * 1024, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else null }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
        else => return err,
    };
    defer if (prior_status) |*value| value.deinit(client.allocator);
    const prior_job: ?Job = if (prior_status) |value| try std.json.parseFromSliceLeaky(Job, scratch, value.body, .{ .allocate = .alloc_always }) else null;
    if (prior_job == null or prior_job.?.wal_lsn <= job.wal_lsn) {
        var status_write = client.putObject(store.opened.bucket, status_key, try std.json.Stringify.valueAlloc(scratch, job, .{}), .{ .if_none_match = prior_status == null, .if_match_etag = if (prior_status) |value| value.metadata.etag orelse return error.MissingObjectEtag else null, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else null }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => null,
            else => return err,
        };
        if (status_write) |*value| value.deinit(client.allocator);
    }
    if (job.state != .ready) return error.IndexRebuilding;
    const encoded = try std.json.Stringify.valueAlloc(scratch, job.declarations, .{});
    return std.json.parseFromSliceLeaky([]const Declared, a, encoded, .{ .allocate = .alloc_always });
}
fn releaseClaim(a: A, store: *Store, key: []const u8, etag: []const u8, previous: Job, io: std.Io) !void {
    const protection = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(protection);
    const Cleanup = struct {
        deadline: u64,
        fn cancelled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            return @import("antfly_platform").time.monotonicNs() >= self.deadline;
        }
    };
    var cleanup: Cleanup = .{ .deadline = @import("antfly_platform").time.monotonicNs() +| std.time.ns_per_s };
    var record = previous;
    record.state = .failed;
    record.lease_until_ms = 0;
    record.next_retry_ms = 0;
    record.last_error = "interrupted";
    record.declarations = &.{};
    const bytes = try std.json.Stringify.valueAlloc(a, record, .{});
    defer a.free(bytes);
    var client = store.opened.client;
    const token: local.storage_object_storage.CancellationToken = .{ .ptr = &cleanup, .is_cancelled_fn = Cleanup.cancelled };
    var observed: ?local.storage_object_storage.GetResult = null;
    defer if (observed) |*value| value.deinit(client.allocator);
    const expected = if (etag.len != 0) etag else recovered: {
        observed = try client.getObject(store.opened.bucket, key, .{ .max_response_bytes = 2 * 1024 * 1024, .cancellation = token });
        var parsed = try std.json.parseFromSlice(Job, a, observed.?.body, .{});
        defer parsed.deinit();
        const owned = parsed.value;
        if (owned.state != .building or !std.mem.eql(u8, &owned.owner_token, &previous.owner_token) or !std.mem.eql(u8, &owned.cut, &previous.cut) or owned.attempts != previous.attempts) return error.PreconditionFailed;
        break :recovered observed.?.metadata.etag orelse return error.MissingObjectEtag;
    };
    var written = try client.putObject(store.opened.bucket, key, bytes, .{ .if_match_etag = expected, .cancellation = token });
    written.deinit(client.allocator);
}
fn buildSegments(a: A, out: A, table: local.common_topology_records.TableRecord, overlay: *const overlay_api.Overlay, declarations: []const Declared, store: *stores.ArtifactStore, wrapper: *Store, context: Context, options: local.inference_managed_embedder.InitOptions) ![]const Declared {
    const io = context.io orelse return error.UnsupportedQueryRequest;
    const token = cancellation(context);
    const configs = try std.json.parseFromSliceLeaky(V, out, table.indexes_json, .{});
    const ids = try out.alloc([]const u8, overlay.rows.count());
    var iterator = overlay.rows.keyIterator();
    for (ids) |*id| id.* = iterator.next().?.*;
    std.mem.sort([]const u8, ids, {}, struct {
        fn less(_: void, l: []const u8, r: []const u8) bool {
            return std.mem.lessThan(u8, l, r);
        }
    }.less);
    var result: std.ArrayList(Declared) = .empty;
    for (declarations) |declaration| {
        if (declaration.artifact.kind != .vector_segment and declaration.artifact.kind != .sparse_segment) continue;
        try context.ensureActive();
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        const path = try std.fmt.allocPrintSentinel(a, "/tmp/antfly-lake-recent-{s}", .{std.fmt.bytesToHex(random, .lower)}, 0);
        defer a.free(path);
        try std.Io.Dir.cwd().createDir(io, path, .fromMode(0o700));
        defer std.Io.Dir.cwd().deleteTree(io, path) catch {};
        const config = configs.object.get(declaration.name) orelse return error.InvalidTableIndexMetadata;
        var producer = try @import("lake_vector_enrichment.zig").Producer.init(a, declaration.name, declaration.binding.column_bindings[0], config, options);
        defer producer.deinit();
        producer.memo = .{ .store = wrapper, .table_id = table.table_id, .recipe = local.metadata_lake_index_catalog.desiredFingerprint(table), .context = context };
        var budget: local.sql_memory_budget = .{ .backing = a, .limit = 256 * 1024 * 1024 };
        var output_bytes: u64 = 512 * 1024 * 1024;
        const bytes = if (declaration.artifact.kind == .vector_segment) dense_build: {
            const archive = try dense.loadRoot(out, store.*, declaration.artifact, token, null);
            const hbc = local.storage_hbc_adapter;
            var hbc_config: hbc.HBCConfig = .{ .dims = archive.dims };
            hbc_config.metric = std.meta.stringToEnum(@TypeOf(hbc_config.metric), archive.metric) orelse return error.InvalidIndexConfig;
            var vectors = try local.storage_lsm_backend.Backend.open(budget.allocator(), try std.fmt.allocPrint(out, "{s}/vectors", .{path}), .{ .flush_threshold = 8 * 1024 * 1024 });
            defer vectors.close();
            var index = try hbc.HBCIndex.openWithLsmOptions(budget.allocator(), path, hbc_config, .{});
            defer index.close();
            index.setIo(io);
            var loader: dense.VectorLoader = .{ .backend = &vectors, .dims = archive.dims, .context = context };
            index.setExternalVectorLoader(&loader, dense.VectorLoader.load);
            index.setExternalVectorScratchLoader(&loader, dense.VectorLoader.loadInto);
            index.setExternalVectorBatchScratchLoader(&loader, dense.VectorLoader.loadMany);
            index.setExperimentalPostingAuthorityTransitionPermitted(true);
            try index.beginBulkIngestSession();
            var finished = false;
            defer if (!finished) index.abortBulkIngestSession();
            var used: std.AutoHashMapUnmanaged(u64, void) = .empty;
            defer used.deinit(budget.allocator());
            var start: usize = 0;
            while (start < ids.len) : (start += @min(@as(usize, 128), ids.len - start)) {
                var page_arena = std.heap.ArenaAllocator.init(budget.allocator());
                defer page_arena.deinit();
                const pa = page_arena.allocator();
                var writes: std.ArrayList(hbc.BatchInsertItem) = .empty;
                var unit_records: std.StringHashMapUnmanaged([]const u8) = .empty;
                for (ids[start..@min(start + 128, ids.len)]) |id| {
                    try context.ensureActive();
                    const row = overlay.row(id).?;
                    for (try producer.units(pa, row)) |unit| {
                        const vector = (try producer.denseUnit(pa, unit, archive.dims)) orelse continue;
                        const key = try @import("lake_enrichment_units.zig").identity(pa, id, unit);
                        if (unit.chunked) {
                            try unit_records.put(pa, key, try @import("lake_enrichment_units.zig").recordJson(pa, unit));
                            try unit_records.put(pa, try @import("lake_enrichment_units.zig").sourceKey(pa, id, unit.source_ordinal), try @import("lake_enrichment_units.zig").sourceJson(pa, unit));
                        }
                        const vector_id = dense.vectorId(key);
                        if ((try used.getOrPut(budget.allocator(), vector_id)).found_existing) return error.NativeLakeVectorIdentityCollision;
                        try writes.append(pa, .{ .vector_id = vector_id, .vector = vector, .metadata = key });
                    }
                }
                var transaction = try vectors.beginBatchWithOptions(.{ .mode = .bulk_ingest });
                errdefer transaction.abort();
                for (writes.items) |write| try transaction.put(.{ .name = "exact_vectors" }, write.metadata, try @import("antfly_vector").codec.encodePackedF32BytesAlloc(pa, write.vector));
                var records = unit_records.iterator();
                while (records.next()) |record| try transaction.put(.{ .name = "lake_units" }, record.key_ptr.*, record.value_ptr.*);
                try transaction.commit();
                try context.ensureActive();
                try index.batchInsertWithMetadataOptions(writes.items, .{ .assume_absent_ids = true, .bulk_ingest = true, .skip_vector_store = true });
            }
            try index.finishBulkIngestSessionWithOptions(.{});
            finished = true;
            try index.finalizeExperimentalPostingGenerationAtAppliedSequence(0, .{ .flatten = true, .make_authoritative = true });
            var checkpoint = (try index.nativeBackupGeneration(a, 0)) orelse return error.InvalidNativeLakeDenseRoot;
            defer checkpoint.deinit();
            const postings = try dense.publishGeneration(a, out, &checkpoint, store, token, &output_bytes, null);
            var vector_checkpoint = try vectors.pinNativeCheckpoint();
            defer vector_checkpoint.deinit();
            const vector_generation = try files.publishCheckpoint(a, out, &vector_checkpoint, store, token, &output_bytes, null);
            var root = archive;
            root.generation = try dense.combineVectors(out, postings, vector_generation);
            root.file_states = &.{};
            break :dense_build try std.json.Stringify.valueAlloc(out, root, .{});
        } else sparse_build: {
            const archive = try sparse.loadRoot(out, store.*, declaration.artifact, token, null);
            var index = try local.sparse_sparse.SparseIndex.open(budget.allocator(), path, .{ .lsm_options = .{ .flush_threshold = 8 * 1024 * 1024 } });
            defer index.close();
            try index.beginBulkIngestSession();
            var finished = false;
            defer if (!finished) index.abortBulkIngestSession();
            var start: usize = 0;
            while (start < ids.len) : (start += @min(@as(usize, 128), ids.len - start)) {
                var page_arena = std.heap.ArenaAllocator.init(budget.allocator());
                defer page_arena.deinit();
                const pa = page_arena.allocator();
                var writes: std.ArrayList(local.sparse_sparse.SparseWrite) = .empty;
                var unit_records: std.StringHashMapUnmanaged([]const u8) = .empty;
                for (ids[start..@min(start + 128, ids.len)]) |id| {
                    try context.ensureActive();
                    const row = overlay.row(id).?;
                    for (try producer.units(pa, row)) |unit| {
                        const vector = (try producer.sparseUnit(pa, unit)) orelse continue;
                        const key = try @import("lake_enrichment_units.zig").identity(pa, id, unit);
                        if (unit.chunked) {
                            try unit_records.put(pa, key, try @import("lake_enrichment_units.zig").recordJson(pa, unit));
                            try unit_records.put(pa, try @import("lake_enrichment_units.zig").sourceKey(pa, id, unit.source_ordinal), try @import("lake_enrichment_units.zig").sourceJson(pa, unit));
                        }
                        try writes.append(pa, .{ .doc_id = key, .vec = .{ .indices = vector.indices, .values = vector.values } });
                    }
                }
                if (unit_records.count() != 0) {
                    var transaction = try index.backendStore().beginBatch();
                    errdefer transaction.abort();
                    var records = unit_records.iterator();
                    while (records.next()) |record| try transaction.put(try std.fmt.allocPrint(pa, "lake-unit:{s}", .{record.key_ptr.*}), record.value_ptr.*);
                    try transaction.commit();
                }
                try index.batchWithOptions(writes.items, &.{}, .{ .prefer_bulk_build = true, .assume_new_doc_ids = true, .backend_batch_options = .{ .mode = .bulk_ingest } });
            }
            try index.finishBulkIngestSessionWithOptions(.{});
            finished = true;
            var checkpoint = try index.pinNativeCheckpoint();
            defer checkpoint.deinit();
            var root = archive;
            root.generation = try files.publishCheckpoint(a, out, &checkpoint, store, token, &output_bytes, null);
            root.file_states = &.{};
            break :sparse_build try std.json.Stringify.valueAlloc(out, root, .{});
        };
        try context.ensureActive();
        var metadata = try store.putWithCancellation(bytes, token);
        defer metadata.deinit(store.allocator);
        var recent = declaration;
        recent.artifact.artifact_id = try out.dupe(u8, metadata.artifact_id);
        recent.artifact.checksum = try out.dupe(u8, metadata.checksum);
        recent.artifact.byte_len = metadata.byte_len;
        try result.append(out, recent);
    }
    return result.toOwnedSlice(out);
}
pub fn runtimeDomain(declaration: Declared) ![32]u8 {
    return ((try stores.uploadScopeFromArtifactId(declaration.artifact.artifact_id)) orelse return error.InvalidArtifactUploadScope).domain;
}

pub fn status(a: A, store: *Store, table: local.common_topology_records.TableRecord, context: Context) ![]u8 {
    const key = try std.fmt.allocPrint(a, "{s}{s}recent-search/{d}/status.json", .{ store.opened.prefix, if (store.opened.prefix.len == 0) "" else "/", table.table_id });
    defer a.free(key);
    var client = store.opened.client;
    var saved = client.getObject(store.opened.bucket, key, .{ .max_response_bytes = 2 * 1024 * 1024, .cancellation = if (context.cancellation) |token| .{ .ptr = token.ptr, .is_cancelled_fn = token.is_cancelled_fn } else null }) catch |err| switch (err) {
        error.NotFound, error.ObjectNotFound, error.FileNotFound => return std.json.Stringify.valueAlloc(a, .{ .state = "idle", .table_id = table.table_id }, .{}),
        else => return err,
    };
    defer saved.deinit(client.allocator);
    var parsed = try std.json.parseFromSlice(Job, a, saved.body, .{});
    defer parsed.deinit();
    const job = parsed.value;
    if (job.table_id != table.table_id or job.object_generation != table.object_storage_generation or !std.mem.eql(u8, &job.desired, &local.metadata_lake_index_catalog.desiredFingerprint(table))) return std.json.Stringify.valueAlloc(a, .{ .state = "recipe_changed", .table_id = table.table_id }, .{});
    return std.json.Stringify.valueAlloc(a, job, .{ .emit_null_optional_fields = false });
}
pub fn collect(store: *Store, table: u64, context: Context) !void {
    try @import("lake_expiring_objects.zig").collect(std.heap.smp_allocator, store, table, "recent-search", context);
    try @import("lake_expiring_objects.zig").collect(std.heap.smp_allocator, store, table, "row-enrichment", context);
    var artifacts = store.artifactStore();
    const Visitor = struct {
        store: *stores.ArtifactStore,
        fn visit(raw: *anyopaque, _: stores.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.store.delete(id);
        }
    };
    var visitor: Visitor = .{ .store = &artifacts };
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    try artifacts.visitScopedUploads(scopeDomain(table, store.identity), .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_cutoff = now -| 30_000, .max_entries = 128 }, cancellation(context));
}

test "external lake interrupted enrichment claims release conditionally without replacing a newer worker" {
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-enrichment-release");
    defer directory.cleanup();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    defer a.free(json);
    var config = try local.common_config.Config.parseFromSlice(a, json);
    defer config.deinit();
    var store = try Store.open(a, &config, null, false);
    defer store.deinit();
    var client = store.opened.client;
    const key = "recent-search/7/claim.json";
    const job: Job = .{ .table_id = 7, .object_generation = 0, .wal_lsn = 8, .archive_generation = 1, .desired = @splat(1), .cut = @splat(2), .lease_until_ms = 10000, .expires_ms = 20000 };
    const bytes = try std.json.Stringify.valueAlloc(a, job, .{});
    defer a.free(bytes);
    var claimed = try client.putObject(store.opened.bucket, key, bytes, .{});
    defer claimed.deinit(client.allocator);
    try releaseClaim(a, &store, key, claimed.etag.?, job, std.testing.io);
    var released = try client.getObject(store.opened.bucket, key, .{});
    defer released.deinit(client.allocator);
    var state = try std.json.parseFromSlice(Job, a, released.body, .{});
    defer state.deinit();
    try std.testing.expect(state.value.state == .failed);
    try std.testing.expectEqual(@as(u64, 0), state.value.lease_until_ms);
    try std.testing.expectEqual(@as(u64, 0), state.value.next_retry_ms);
    var next_claim = job;
    next_claim.attempts = 1;
    const next_bytes = try std.json.Stringify.valueAlloc(a, next_claim, .{});
    defer a.free(next_bytes);
    var replacement = try client.putObject(store.opened.bucket, key, next_bytes, .{ .if_match_etag = released.metadata.etag.? });
    defer replacement.deinit(client.allocator);
    try std.testing.expectError(error.PreconditionFailed, releaseClaim(a, &store, key, claimed.etag.?, job, std.testing.io));
    var current = try client.getObject(store.opened.bucket, key, .{});
    defer current.deinit(client.allocator);
    try std.testing.expectEqualStrings(next_bytes, current.body);
    // Losing the successful PUT response must not strand our lease. Recover
    // its token and ETag, while refusing a newer worker even without an ETag.
    try std.testing.expectError(error.PreconditionFailed, releaseClaim(a, &store, key, "", job, std.testing.io));
    try releaseClaim(a, &store, key, "", next_claim, std.testing.io);
    var recovered = try client.getObject(store.opened.bucket, key, .{});
    defer recovered.deinit(client.allocator);
    var recovered_state = try std.json.parseFromSlice(Job, a, recovered.body, .{});
    defer recovered_state.deinit();
    try std.testing.expectEqual(@as(u64, 0), recovered_state.value.lease_until_ms);
    try std.testing.expectEqualStrings("interrupted", recovered_state.value.last_error.?);
}

// A ten-minute generation can serve a five-minute cut, but cannot be bound
// into an hour-long cursor, even while it remains ready and unexpired.
test "external lake recent vectors require retention through the entire query cut" {
    const alloc = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("lake-recent-retention");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } } }, .{});
    var config = try local.common_config.Config.parseFromSlice(alloc, json);
    defer config.deinit();
    var store = try Store.open(alloc, &config, null, false);
    defer store.deinit();
    const table: local.common_topology_records.TableRecord = .{ .table_id = 4, .name = "history", .schema_json = "{}", .indexes_json = "{}" };
    const publication: local.metadata_lake_index_catalog.Publication = .{ .generation = 1, .token = @splat(1), .signature = .{ .desired = @splat(1), .source = @splat(2), .credentials = @splat(3), .store = @splat(4) }, .published_at_ms = 1, .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://bucket/lake", .snapshot_id = "snapshot", .schema_fingerprint = "schema", .file_inventory_artifact = "inventory" } }, .inventory = .{ .artifact_id = "inventory", .kind = .external_base_source, .byte_len = 42, .checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" } };
    const overlay = try overlay_api.Overlay.init(a, .{ .lsn = 8, .key_fields = &.{"id"}, .changes = &.{} });
    const cut = try cutDigest(a, table, publication, &overlay);
    const key = try jobKey(a, &store, table.table_id, cut);
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    const job: Job = .{ .state = .ready, .table_id = table.table_id, .object_generation = table.object_storage_generation, .wal_lsn = 8, .archive_generation = publication.generation, .desired = local.metadata_lake_index_catalog.desiredFingerprint(table), .cut = cut, .expires_ms = now + 600_000 };
    var client = store.opened.client;
    var saved = try client.putObject(store.opened.bucket, key, try std.json.Stringify.valueAlloc(a, job, .{}), .{});
    defer saved.deinit(client.allocator);
    try std.testing.expectEqual(@as(usize, 0), (try prepare(a, table, publication, &overlay, &.{}, &store, .{}, .{}, false, now + 300_000)).len);
    try std.testing.expectError(error.IndexRebuilding, prepare(a, table, publication, &overlay, &.{}, &store, .{}, .{}, false, now + 3_600_000));
    try std.testing.expectError(error.IndexRebuilding, prepare(a, table, publication, &overlay, &.{}, &store, .{}, .{}, false, job.expires_ms));
}
