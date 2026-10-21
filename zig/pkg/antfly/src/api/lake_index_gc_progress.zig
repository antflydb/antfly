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

//! Durable bounded mark frontier and live set, stored in native immutable pages.
//! Metadata CAS binds checkpoints to the exact collection cut. Uploads for the
//! collector share a private attempt excluded from sweeping until completion.
const std = @import("std");
const local = @import("antfly_local_sources");
const gc = @import("lake_index_gc.zig");
const lifecycle = @import("../metadata/lake_index_lifecycle.zig");
const stores = @import("../serverless/artifacts/store.zig");
const artifacts = @import("lake_index_aggregate_artifact.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const page_store = @import("../serverless/graph_segment/page_store.zig");
const A = std.mem.Allocator;
const Ref = local.serverless_manifest_artifact_ref.ArtifactRef;
const Checkpoint = struct {
    version: u16 = 1,
    domain: [32]u8,
    floor: u64,
    attempt: [16]u8,
    cutoff: u64,
    marks: ?tree.Ref = null,
    work: ?tree.Ref = null,
    next: u64 = 0,
    end: u64 = 0,
    marked: u64 = 0,
    sweep_after: ?[]const u8 = null,
    enumeration: ?[]const u8 = null,
    deferred_retention: bool = false,
};
pub const Job = union(enum) {
    publication: local.metadata_lake_index_catalog.Publication,
    generation_page: struct { ref: local.serverless_manifest_artifact_ref.ProviderGenerationRef, depth: u8 },
    artifact: Ref,
    contribution_page: Ref,
    chunk: artifacts.ChunkRef,
    text_directory: artifacts.ChunkRef,
    text_root: artifacts.ChunkRef,
    text_manifest: struct { ref: artifacts.ChunkRef, seekable: bool, maintenance_only: bool = false },
    page: struct { ref: tree.Ref, ordered_rows: bool, native_files: bool = false, native_generation: bool = false, native_chunks: bool = false, contributions: bool = false, text_files: ?struct { seekable: bool, maintenance_only: bool } = null },
};
pub const Progress = struct {
    collector: *gc.Collector,
    a: A,
    work_a: A,
    value: Checkpoint,
    pages: *page_store.PageStore,
    page_cache: *tree.Cache,
    old_end: u64,
    previous: ?[]const u8 = null,
    queued_bytes: usize = 0,
    queued: std.ArrayList(tree.Mutation) = .empty,
    marked: std.StringHashMapUnmanaged(artifacts.ChunkRef) = .empty,
    pub fn enqueue(self: *Progress, job: Job) !void {
        const key = try self.a.alloc(u8, 8);
        std.mem.writeInt(u64, key[0..8], self.value.end, .big);
        const bytes = try std.json.Stringify.valueAlloc(self.a, job, .{});
        self.queued_bytes += bytes.len + key.len;
        try self.queued.append(self.a, .{ .key = key, .value = bytes });
        self.value.end = try std.math.add(u64, self.value.end, 1);
    }
    fn contains(self: *Progress, id: []const u8) !bool {
        if (self.marked.contains(id)) return true;
        var cursor = try tree.Cursor.init(self.work_a, self.page_cache.store(), self.value.marks, id, null);
        defer cursor.deinit();
        const entry = try cursor.next() orelse return false;
        return std.mem.eql(u8, entry.key, id);
    }
    pub fn record(self: *Progress, ref: artifacts.ChunkRef) !bool {
        try self.collector.check();
        try stores.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum);
        const scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeGcReference;
        if (!std.mem.eql(u8, &scope.domain, &self.value.domain)) return error.InvalidNativeLakeGcReference;
        if (self.marked.get(ref.artifact_id)) |previous| {
            if (previous.byte_len != ref.byte_len) return error.InvalidNativeLakeGcReference;
            return false;
        }
        if (try self.contains(ref.artifact_id)) return false;
        const owned = try self.a.dupe(u8, ref.artifact_id);
        try self.marked.put(self.a, owned, .{ .artifact_id = owned, .checksum = owned[7..71], .byte_len = ref.byte_len });
        self.value.marked = try std.math.add(u64, self.value.marked, 1);
        return true;
    }
    pub fn expand(self: *Progress, ref: artifacts.ChunkRef, kind: []const u8) !bool {
        _ = try self.record(ref);
        const key = try std.fmt.allocPrint(self.a, "expanded:{s}:{s}", .{ kind, ref.artifact_id });
        if (try self.contains(key)) return false;
        try self.marked.put(self.a, key, .{ .artifact_id = key, .checksum = "", .byte_len = 0 });
        return true;
    }
    fn process(self: *Progress, job: Job) !void {
        const collector = self.collector;
        var scratch = std.heap.ArenaAllocator.init(self.work_a);
        defer scratch.deinit();
        const a = scratch.allocator();
        switch (job) {
            .chunk => |chunk| {
                _ = try self.record(chunk);
            },
            .artifact => |artifact| try collector.markArtifact(a, artifact),
            .text_directory => |ref| try collector.markTextDirectory(a, ref),
            .text_root => |ref| try collector.markTextRootAuto(a, ref),
            .text_manifest => |manifest| try collector.markTextManifestMode(a, manifest.ref, manifest.seekable, manifest.maintenance_only),
            .contribution_page => |page| {
                if (!try self.expand(.{ .artifact_id = page.artifact_id, .checksum = page.checksum, .byte_len = page.byte_len }, "contributions")) return;
                try stores.chargeReadBudget(&collector.remaining_reads, page.byte_len);
                for (try @import("lake_index_directory.zig").loadContributionPage(a, collector.store, page, collector.cancellation(), null)) |contribution| try self.enqueue(.{ .artifact = contribution.artifact });
            },
            .generation_page => |page| {
                const ref = page.ref;
                if (!try self.expand(.{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, "cloud-generation-page")) return;
                const object = collector.store.providerStore() orelse return error.CloudGenerationStoreUnavailable;
                var reader: @import("../serverless/artifacts/cloud_generations.zig").Reader = .{ .client = object.client, .bucket = object.bucket, .prefix = object.prefix, .cache_limit = 0 };
                defer reader.deinit();
                try stores.chargeReadBudget(&collector.remaining_reads, ref.byte_len);
                const node = try reader.read(a, ref, page.depth, collector.cancellation());
                for (node.children) |child| try self.enqueue(.{ .generation_page = .{ .ref = child.ref, .depth = page.depth + 1 } });
            },
            .publication => |publication| {
                if (publication.cloud_generations) |cloud| try self.enqueue(.{ .generation_page = .{ .ref = cloud.root, .depth = 0 } });
                try self.enqueue(.{ .chunk = .{ .artifact_id = publication.inventory.artifact_id, .checksum = publication.inventory.checksum, .byte_len = publication.inventory.byte_len } });
                // Resumable GC expands publications directly instead of calling
                // Collector.markPublication. Protect navigation independently of
                // directory deduplication, including retained reader-only roots.
                if (publication.source_navigation) |ref| {
                    if (try self.expand(.{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, "source-navigation")) {
                        try stores.chargeReadBudget(&collector.remaining_reads, ref.byte_len);
                        const navigation = try @import("lake_source_navigation.zig").load(a, collector.store, ref, collector.cancellation(), null);
                        if (!std.mem.eql(u8, &navigation.domain, &self.value.domain)) return error.InvalidNativeLakeGcReference;
                        for (@import("lake_source_navigation.zig").dependencies(navigation)) |root| if (root) |page| try self.enqueue(.{ .page = .{ .ref = page, .ordered_rows = false } });
                    }
                }
                var hydrated = publication;
                if (publication.directory) |directory| {
                    if (!try self.expand(.{ .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len }, "directory")) return;
                    try stores.chargeReadBudget(&collector.remaining_reads, directory.byte_len);
                    const document = try @import("lake_index_directory.zig").loadDocument(a, collector.store, .{ .kind = .external_base_source, .artifact_id = directory.artifact_id, .checksum = directory.checksum, .byte_len = directory.byte_len }, collector.cancellation(), null);
                    hydrated.declarations = document.declarations;
                    hydrated.file_contributions = document.file_contributions;
                    if (document.contribution_index) |root| try self.enqueue(.{ .page = .{ .ref = root, .ordered_rows = false, .contributions = true } });
                    for (document.contribution_pages) |page| try self.enqueue(.{ .contribution_page = page });
                }
                for (hydrated.declarations) |declaration| try self.enqueue(.{ .artifact = declaration.artifact });
                for (hydrated.file_contributions) |contribution| try self.enqueue(.{ .artifact = contribution.artifact });
            },
            .page => |job_page| {
                const page = job_page.ref;
                if (!try self.expand(.{ .artifact_id = &try page_store.PageStore.identity(self.value.domain, page), .checksum = &std.fmt.bytesToHex(&page.digest, .lower), .byte_len = page.bytes }, if (job_page.text_files) |files| (if (files.maintenance_only) "text-maintenance-file-page" else "text-serving-file-page") else if (job_page.native_generation) "native-generation-files" else if (job_page.native_chunks) "native-generation-chunks" else if (job_page.native_files) "native-file-state-page" else if (job_page.contributions) "contribution-index" else if (job_page.ordered_rows) "ordered-page" else "page")) return;
                var write_bytes: u64 = 0;
                var pages: page_store.PageStore = .{ .domain = self.value.domain, .artifacts = &collector.store, .cancellation = collector.cancellation(), .remaining_read_bytes = &collector.remaining_reads, .remaining_write_bytes = &write_bytes };
                const Visitor = struct {
                    progress: *Progress,
                    ordered: bool,
                    contributions: bool,
                    native_files: bool,
                    native_generation: bool,
                    native_chunks: bool,
                    text_files: @FieldType(@FieldType(Job, "page"), "text_files"),
                    a: A,
                    pub fn child(visitor: *@This(), ref: tree.Ref) !void {
                        try visitor.progress.enqueue(.{ .page = .{ .ref = ref, .ordered_rows = visitor.ordered, .native_files = visitor.native_files, .native_generation = visitor.native_generation, .native_chunks = visitor.native_chunks, .contributions = visitor.contributions, .text_files = visitor.text_files } });
                    }
                    pub fn record(visitor: *@This(), key: []const u8, bytes: []const u8) !void {
                        if (visitor.native_generation) {
                            const file = try @import("lake_index_native_directory.zig").decodeFile(visitor.a, key, bytes, visitor.progress.value.domain);
                            if (file.chunk_root) |chunk_page| try visitor.progress.enqueue(.{ .page = .{ .ref = chunk_page, .ordered_rows = false, .native_chunks = true } });
                            return;
                        }
                        if (visitor.native_chunks) {
                            if (key.len != 8) return error.InvalidNativeLakeGcReference;
                            try visitor.progress.enqueue(.{ .chunk = try @import("lake_index_native_directory.zig").decodeChunk(visitor.a, bytes, visitor.progress.value.domain) });
                            return;
                        }
                        if (visitor.native_files) {
                            const file = try @import("lake_index_native_state.zig").decodeDirectoryFile(visitor.a, key, bytes, visitor.progress.value.domain);
                            for (file.docs) |ref| try visitor.progress.enqueue(.{ .chunk = ref });
                            return;
                        }
                        if (visitor.text_files) |files| {
                            if (key.len != 36 or std.mem.readInt(u32, key[32..36], .big) != 0) return error.InvalidNativeLakeGcReference;
                            const record_ = try @import("lake_index_text_directory.zig").decodeFileRecord(visitor.a, key[0..32].*, bytes, visitor.progress.value.domain);
                            try visitor.progress.enqueue(.{ .text_manifest = .{ .ref = record_.manifest, .seekable = files.seekable, .maintenance_only = files.maintenance_only } });
                            return;
                        }
                        if (visitor.contributions) {
                            const value = try std.json.parseFromSliceLeaky(local.metadata_lake_index_catalog.FileContribution, visitor.a, bytes, .{ .allocate = .alloc_always });
                            try @import("lake_index_contributions.zig").validate(value);
                            if (!std.mem.eql(u8, key, &@import("lake_index_contributions.zig").identity(value))) return error.InvalidLakeIndexCatalog;
                            try visitor.progress.enqueue(.{ .artifact = value.artifact });
                            return;
                        }
                        if (!visitor.ordered) return;
                        if (try @import("lake_index_ordered_rows.zig").coverReference(visitor.a, visitor.progress.value.domain, bytes)) |cover| try visitor.progress.enqueue(.{ .chunk = cover.block });
                    }
                };
                var visitor: Visitor = .{ .progress = self, .ordered = job_page.ordered_rows, .native_files = job_page.native_files, .native_generation = job_page.native_generation, .native_chunks = job_page.native_chunks, .contributions = job_page.contributions, .text_files = job_page.text_files, .a = a };
                try tree.walkPage(a, pages.store(), page, &visitor);
            },
        }
    }
    fn persist(self: *Progress) !Ref {
        const changes = try self.a.alloc(tree.Mutation, self.marked.count());
        var iterator = self.marked.iterator();
        for (changes) |*change| {
            const entry = iterator.next().?;
            const bytes = try self.a.alloc(u8, 8);
            std.mem.writeInt(u64, bytes[0..8], entry.value_ptr.byte_len, .big);
            change.* = .{ .key = entry.key_ptr.*, .value = bytes };
        }
        std.mem.sort(tree.Mutation, changes, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.order(u8, l.key, r.key) == .lt;
            }
        }.less);
        self.value.marks = try tree.apply(self.work_a, self.page_cache.store(), self.value.marks, changes);
        self.value.work = try tree.apply(self.work_a, self.page_cache.store(), self.value.work, self.queued.items);
        const bytes = try std.json.Stringify.valueAlloc(self.a, self.value, .{});
        const upload = try self.pages.artifacts.putScoped(.{ .domain = self.pages.domain, .attempt = self.pages.attempt }, bytes, self.collector.cancellation());
        defer if (upload.provider_version) |version| self.pages.artifacts.allocator.free(version);
        return .{ .kind = .external_base_source, .artifact_id = upload.artifact_id, .checksum = upload.checksum, .byte_len = upload.byte_len };
    }
};
/// Once a store has no serving, reader or builder roots, cleanup needs no
/// immutable mark checkpoint. Otherwise retention on GC's own checkpoints can
/// create a permanent chain of protected bookkeeping objects after table drop.
fn sweepEmptyStore(collector: *gc.Collector, state: lifecycle.State) !?gc.Result {
    const object = collector.store.providerStore() orelse return null;
    if (object.fs_client != null or object.client.vtable.list_object_versions == null) return null;
    if (state.collection.?.progress != null) return null;
    if (state.pending) |pending| if (std.mem.eql(u8, &pending.signature.store, &collector.identity)) return null;
    const roots = try state.gcPublications(collector.a);
    defer collector.a.free(roots);
    for (roots) |root| {
        if (!std.mem.eql(u8, &root.signature.store, &collector.identity) or !std.meta.eql(root.namespace, collector.namespace)) continue;
        const retired = for (state.collection.?.retired) |generation| {
            if (generation == root.generation) break true;
        } else false;
        if (!retired) return null;
    }
    const Visitor = struct {
        collector: *gc.Collector,
        deferred: bool = false,
        fn visit(raw: *anyopaque, scope: stores.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const c = self.collector;
            try c.check();
            if (scope.fencingToken() < c.upload_floor or scope.fencingToken() >= c.upload_cutoff) return;
            c.result.eligible += 1;
            c.store.deleteWithCancellation(id, c.cancellation()) catch |err| switch (err) {
                error.ArtifactRetentionActive, error.ArtifactVersionDeletionPaused => {
                    self.deferred = true;
                    return;
                },
                error.FileNotFound => {},
                else => return err,
            };
            c.result.deleted += 1;
        }
    };
    var visitor: Visitor = .{ .collector = collector };
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(collector.table, collector.identity, collector.namespace);
    collector.store.visitScopedUploads(domain, .{ .ptr = &visitor, .visit = Visitor.visit, .fencing_floor = collector.upload_floor, .fencing_cutoff = collector.upload_cutoff, .max_entries = @max(1, @min(128, collector.options.max_deleted)) }, collector.cancellation()) catch |err| switch (err) {
        error.ArtifactEnumerationPaused => visitor.deferred = true,
        else => return err,
    };
    try collector.authority.apply(collector.table, .{ .finish_collection = .{ .token = collector.token, .now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, .retain_store = visitor.deferred } });
    collector.result.complete = true;
    return collector.result;
}
pub fn run(collector: *gc.Collector, state: lifecycle.State) !gc.Result {
    if (try sweepEmptyStore(collector, state)) |result| return result;
    // Background callers commonly use an arena. Own the backing here so
    // releasing page/job scratch actually frees memory during a bounded pass.
    var budget: local.sql_memory_budget = .{ .backing = std.heap.page_allocator, .limit = 128 * 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(budget.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    const collection = state.collection.?;
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(collector.table, collector.identity, collector.namespace);
    if (collection.upload_floor == collection.upload_cutoff) {
        try collector.authority.apply(collector.table, .{ .finish_collection = .{ .token = collector.token, .now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms } });
        collector.result.complete = true;
        return collector.result;
    }
    // Separate bounded checkpoint I/O from payload traversal, leaving capacity
    // to commit progress after the traversal budget has been consumed.
    var checkpoint_reads: u64 = 64 * 1024 * 1024;
    var checkpoint_writes: u64 = 64 * 1024 * 1024;
    const io = collector.context.io orelse (if (@import("builtin").is_test) std.testing.io else return error.UnsupportedOperation);
    const scope = try stores.UploadScope.forPublication(domain, collection.upload_cutoff - 1, io);
    var checkpoint_store = collector.store;
    checkpoint_store.allocator = a;
    checkpoint_store.upload_scope = scope;
    var pages: page_store.PageStore = .{ .domain = domain, .attempt = scope.attempt, .artifacts = &checkpoint_store, .cancellation = collector.cancellation(), .remaining_read_bytes = &checkpoint_reads, .remaining_write_bytes = &checkpoint_writes };
    var value: Checkpoint = .{ .domain = domain, .floor = collection.upload_floor, .cutoff = collection.upload_cutoff, .attempt = scope.attempt };
    if (collection.progress) |ref| {
        if (ref.byte_len > 4096) return error.InvalidNativeLakeGcReference;
        const checkpoint_scope = (try stores.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidNativeLakeGcReference;
        if (!std.mem.eql(u8, &checkpoint_scope.domain, &domain) or checkpoint_scope.fencingToken() != collection.upload_cutoff - 1) return error.InvalidNativeLakeGcReference;
        const bytes = try artifacts.readArtifact(a, collector.store, .{ .artifact_id = ref.artifact_id, .checksum = ref.checksum, .byte_len = ref.byte_len }, collector.cancellation(), null);
        value = try std.json.parseFromSliceLeaky(Checkpoint, a, bytes, .{ .allocate = .alloc_always });
        if (value.version != 1 or value.next > value.end or value.floor != collection.upload_floor or value.cutoff != collection.upload_cutoff or !std.mem.eql(u8, &value.domain, &domain)) return error.InvalidNativeLakeGcReference;
    }
    const progress_scope: stores.UploadScope = .{ .domain = domain, .attempt = value.attempt };
    try progress_scope.validate();
    if (progress_scope.fencingToken() != collection.upload_cutoff - 1) return error.InvalidNativeLakeGcReference;
    pages.attempt = value.attempt;
    checkpoint_store.upload_scope = progress_scope;
    var page_cache: tree.Cache = .{ .alloc = budget.allocator(), .underlying = pages.store() };
    defer page_cache.deinit();
    var progress: Progress = .{ .collector = collector, .a = a, .work_a = budget.allocator(), .value = value, .pages = &pages, .page_cache = &page_cache, .old_end = value.end, .previous = if (collection.progress) |ref| ref.artifact_id else null };
    collector.progress = &progress;
    defer collector.progress = null;
    const publications = try state.gcPublications(a);
    if (collector.store.providerStore()) |object| object.cloud_io = collector.context.io;
    var cloud_union: ?@import("lake_cloud_publication.zig").UnionBinding = if (collector.store.providerStore()) |object| try @import("lake_cloud_publication.zig").UnionBinding.init(collector.a, object, publications, collection.retired, collector.namespace, collector.identity) else null;
    defer if (cloud_union) |*binding| binding.deinit();
    if (collection.progress == null) for (state.stores) |binding| {
        if (std.mem.eql(u8, &binding.identity, &collector.identity)) if (binding.vacuum) |cut| if (cut.marks) |root| try progress.enqueue(.{ .page = .{ .ref = root, .ordered_rows = false } });
    };
    if (collection.progress == null) for (publications) |publication| {
        if (!std.meta.eql(publication.namespace, collector.namespace) or !std.mem.eql(u8, &publication.signature.store, &collector.identity)) continue;
        const retired = for (collection.retired) |generation| {
            if (generation == publication.generation) break true;
        } else false;
        if (!retired) try progress.enqueue(.{ .publication = publication });
    };
    const initial_next = progress.value.next;
    const max_jobs = @min(256, collector.options.max_marked);
    var cursor = try tree.Cursor.initAtRank(a, page_cache.store(), progress.value.work, progress.value.next);
    defer cursor.deinit();
    while (progress.value.next < progress.value.end and progress.value.next - initial_next < max_jobs) {
        if (progress.value.next != initial_next) {
            if (collector.remaining_reads < @min(collector.options.max_read_bytes, 8 * 1024 * 1024) or checkpoint_reads < 16 * 1024 * 1024 or progress.queued_bytes >= 8 * 1024 * 1024) break;
            if (collector.context.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() +| 3 * std.time.ns_per_s >= deadline) break;
        }
        const bytes = if (progress.value.next < progress.old_end) (try cursor.next() orelse return error.InvalidNativeLakeGcReference).value else progress.queued.items[@intCast(progress.value.next - progress.old_end)].value.?;
        var job_arena = std.heap.ArenaAllocator.init(progress.work_a);
        defer job_arena.deinit();
        const job = try std.json.parseFromSliceLeaky(Job, job_arena.allocator(), bytes, .{ .allocate = .alloc_always });
        try progress.process(job);
        progress.value.next += 1;
    }
    collector.result.marked = @intCast(progress.value.marked);
    // Materialize the set before sweeping so membership never needs an
    // in-memory copy of the complete live graph.
    const checkpoint = try progress.persist();
    try collector.authority.apply(collector.table, .{ .checkpoint_collection = .{ .token = collector.token, .previous = progress.previous, .progress = checkpoint, .now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms } });
    progress.previous = checkpoint.artifact_id;
    if (progress.value.next != progress.value.end) return collector.result;
    const Visitor = struct {
        progress: *Progress,
        ids: std.ArrayList([]const u8) = .empty,
        pub fn visit(raw: *anyopaque, _: stores.UploadScope, id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.progress.collector.check();
            try self.ids.append(self.progress.a, try self.progress.a.dupe(u8, id));
        }
        pub fn saveContinuation(raw: *anyopaque, token: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.progress.value.enumeration = try self.progress.a.dupe(u8, token);
        }
    };
    var visitor: Visitor = .{ .progress = &progress };
    var paused = false;
    collector.store.visitScopedUploads(domain, .{ .ptr = &visitor, .visit = Visitor.visit, .checkpoint = Visitor.saveContinuation, .continuation = progress.value.enumeration, .cleanup_staging = true, .after_suffix = progress.value.sweep_after, .fencing_floor = collector.upload_floor, .fencing_cutoff = collector.upload_cutoff, .exclude_attempt = progress.value.attempt, .max_entries = @max(1, @min(256, collector.options.max_deleted)) }, collector.cancellation()) catch |err| {
        if (err != error.ArtifactEnumerationPaused) return err;
        paused = true;
    };
    const sorted = try a.dupe([]const u8, visitor.ids.items);
    std.mem.sort([]const u8, sorted, {}, struct {
        fn less(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.order(u8, left, right) == .lt;
        }
    }.less);
    const found = try a.alloc(bool, sorted.len);
    try tree.containsMany(progress.work_a, page_cache.store(), progress.value.marks, sorted, found);
    for (sorted, found, 0..) |id, live, index| {
        if (index != 0 and std.mem.eql(u8, sorted[index - 1], id)) continue;
        try collector.check();
        if (live) continue;
        collector.result.eligible += 1;
        collector.store.deleteWithCancellation(id, collector.cancellation()) catch |err| switch (err) {
            error.ArtifactRetentionActive => {
                progress.value.deferred_retention = true;
                continue;
            },
            else => return err,
        };
        collector.result.deleted += 1;
    }
    // The lexical cursor follows backend order, independent of batched probes.
    if (visitor.ids.getLastOrNull()) |id| {
        const suffix = try a.alloc(u8, 97);
        @memcpy(suffix[0..32], id[143..175]);
        suffix[32] = '/';
        @memcpy(suffix[33..97], id[7..71]);
        progress.value.sweep_after = suffix;
    }
    if (paused) {
        const next = try progress.persist();
        try collector.authority.apply(collector.table, .{ .checkpoint_collection = .{ .token = collector.token, .previous = progress.previous, .progress = next, .now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms } });
        return collector.result;
    }
    const cloud_checkpoints = if (collector.store.providerStore()) |object| try object.usesObjectVersions(collector.cancellation()) else false;
    try collector.authority.apply(collector.table, .{ .finish_collection = .{ .token = collector.token, .now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, .retain_store = progress.value.deferred_retention or cloud_checkpoints, .vacuum = if (cloud_checkpoints) .{ .domain = domain, .floor = progress.value.floor, .cutoff = progress.value.cutoff, .marks = progress.value.marks } else null } });
    // Legacy nonce staging predates journal-discoverable pending-v2 files.
    // Its advisory migration cleanup must never gate collection completion.
    collector.store.cleanupRetiredScopedTemporaryRange(domain, collector.upload_floor, collector.upload_cutoff, collector.cancellation()) catch |err| {
        std.log.warn("completed lake GC legacy staging cleanup deferred: {t}", .{err});
    };
    if (cloud_checkpoints) {
        collector.result.complete = true;
        return collector.result; // mark pages are now durable vacuum evidence
    }
    // The metadata receipt is durable before private checkpoint reclamation.
    // Interrupted cleanup leaves ordinary below-cutoff orphans for the next GC.
    const Cleanup = struct {
        store: *stores.ArtifactStore,
        fn visit(raw: *anyopaque, _: stores.UploadScope, id: []const u8) !void {
            const cleanup: *@This() = @ptrCast(@alignCast(raw));
            cleanup.store.delete(id) catch |err| switch (err) {
                error.FileNotFound => {},
                else => return err,
            };
        }
    };
    var cleanup: Cleanup = .{ .store = &collector.store };
    collector.store.visitScopedUploads(domain, .{ .ptr = &cleanup, .visit = Cleanup.visit, .only_attempt = progress.value.attempt }, collector.cancellation()) catch |err| {
        std.log.warn("completed lake GC checkpoint cleanup deferred: {t}", .{err});
    };
    collector.store.reclaimRetiredScopedInventory(domain, collector.upload_floor, collector.upload_cutoff, collector.cancellation()) catch |err| {
        std.log.warn("completed lake GC inventory cleanup deferred: {t}", .{err});
    };
    collector.result.complete = true;
    return collector.result;
}

test "external lake global frontier cloud retirement keeps locked stores and drains without protected checkpoint chains" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const Provider = @import("../serverless/artifacts/cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    var object = try @import("../serverless/artifacts/object_store.zig").ObjectStore.initWithClient(a, client, "bucket", "native");
    defer object.deinit();
    var store = object.artifactStore();
    const identity: [32]u8 = @splat(5);
    const namespace: [32]u8 = @splat(8);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(7, identity, namespace);
    const scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    var orphan = try store.putScoped(scope, "retired protected payload", .none);
    defer orphan.deinit(a);
    const Harness = struct {
        a: A,
        state: lifecycle.State,
        fn read(raw: *anyopaque, alloc: A, _: u64, _: local.api_operation.RequestContext) ![]u8 {
            return lifecycle.encode(alloc, @as(*@This(), @ptrCast(@alignCast(raw))).state);
        }
        fn mutate(raw: *anyopaque, _: u64, revision: u64, mutation: lifecycle.Mutation, _: local.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.state.revision != revision) return error.CatalogGenerationChanged;
            self.state = try mutation.apply(self.a, self.state);
        }
    };
    var harness: Harness = .{ .a = ca, .state = .{ .generation = 2, .reader_upload_floor = 1, .namespace = namespace, .dropped = true, .stores = &.{.{ .identity = identity, .locator = .{ .protocol = .gcs, .connection = "cloud", .bucket = "bucket", .prefix = "native", .root = "https://storage.googleapis.com" } }} } };
    var collector: gc.Collector = .{ .a = a, .table = 7, .authority = .{ .ptr = &harness, .context = .{}, .read = Harness.read, .mutate = Harness.mutate }, .store = store, .identity = identity, .context = .{ .io = std.testing.io }, .token = @splat(1), .options = .{ .dry_run = false } };
    const serial = provider.serial;
    const protected = try collector.run();
    try std.testing.expect(protected.complete);
    try std.testing.expectEqual(@as(usize, 0), protected.deleted);
    try std.testing.expectEqual(@as(usize, 1), harness.state.stores.len);
    try std.testing.expectEqual(serial, provider.serial);
    provider.now_ms = provider.until_ms;
    collector.token = @splat(2);
    const drained = try collector.run();
    try std.testing.expect(drained.complete);
    try std.testing.expectEqual(@as(usize, 1), drained.deleted);
    try std.testing.expectEqual(@as(usize, 0), harness.state.stores.len);
    try std.testing.expectEqual(serial, provider.serial);
}

test "external lake global frontier resumable GC retains delete-version and native file-state pages until the last reader retires" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var directory = try local.common_test_directory.TestDirectory.init("delete-proof-resumable-gc");
    defer directory.cleanup();
    var fs = try @import("../serverless/artifacts/fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    const identity: [32]u8 = @splat(7);
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(1, identity, null);
    store.upload_scope = try stores.UploadScope.forPublication(domain, 1, std.testing.io);
    var reads: u64 = 16 * 1024 * 1024;
    var writes: u64 = 16 * 1024 * 1024;
    var pages: page_store.PageStore = .{ .domain = domain, .attempt = store.upload_scope.?.attempt, .artifacts = &store, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var builder: tree.SortedBuilder = .{ .alloc = a, .store = pages.store() };
    defer builder.deinit();
    try builder.add(&@as([32]u8, @splat(0)), "exact provider delete selector");
    const versions = (try builder.finish()).?;
    const navigation: @import("lake_source_navigation.zig").Root = .{ .domain = domain, .source = "source", .schema = "schema", .snapshot = "first", .count = 0, .coverage = .{ .plan = @splat(1), .data = @splat(2) }, .delete_proof = .{ .recipe = @splat(3), .digest = @splat(4), .source = @splat(5), .data = @splat(2), .count = 1, .versions = versions } };
    var nav_ref = try store.put(try std.json.Stringify.valueAlloc(ca, navigation, .{}));
    defer nav_ref.deinit(a);
    var inventory = try store.put("inventory");
    defer inventory.deinit(a);
    var publication: local.metadata_lake_index_catalog.Publication = .{ .reader_protocol = local.metadata_lake_index_catalog.native_reader_protocol, .generation = 1, .token = store.upload_scope.?.attempt, .signature = .{ .desired = @splat(1), .source = @splat(5), .credentials = @splat(2), .store = identity }, .published_at_ms = 1, .base_source = .{ .external_iceberg = .{ .format = .iceberg, .source_uri = "s3://bucket/table", .snapshot_id = "first", .schema_fingerprint = "schema", .file_inventory_artifact = inventory.artifact_id } }, .inventory = .{ .kind = .external_base_source, .artifact_id = inventory.artifact_id, .checksum = inventory.checksum, .byte_len = inventory.byte_len }, .source_navigation = .{ .kind = .doc_values, .metadata_version = 2, .artifact_id = nav_ref.artifact_id, .checksum = nav_ref.checksum, .byte_len = nav_ref.byte_len } };
    var document = try store.put(&@as([96]u8, @splat('x')));
    defer document.deinit(a);
    const native = @import("lake_index_native_state.zig");
    const state_file: native.File = .{ .id = "file", .digest = @splat(3), .count = 1, .docs = &.{.{ .artifact_id = document.artifact_id, .checksum = document.checksum, .byte_len = document.byte_len }} };
    var file_builder: tree.SortedBuilder = .{ .alloc = a, .store = pages.store() };
    defer file_builder.deinit();
    try file_builder.add(&native.Selection.idKey(state_file.id), try std.json.Stringify.valueAlloc(ca, state_file, .{}));
    const file_directory = (try file_builder.finish()).?;
    const sparse_binding: local.serverless_segment_source_binding.Binding = .{ .sidecar_kind = .sparse, .source_kind = .external_iceberg, .row_ref_kind = .external, .source_id = "source", .snapshot_id = "first", .schema_fingerprint = "schema", .index_config_hash = "recipe", .column_bindings = &.{"embedding"} };
    const sparse_root: @import("lake_index_native_sparse.zig").Root = .{ .version = 3, .binding = sparse_binding, .config_json = "{}", .generation = .{ .domain = domain, .files = &.{} }, .file_directory = file_directory };
    var sparse_ref = try store.put(try std.json.Stringify.valueAlloc(ca, sparse_root, .{}));
    defer sparse_ref.deinit(a);
    publication.declarations = &.{.{ .name = "sparse", .binding = sparse_binding, .artifact = .{ .kind = .sparse_segment, .metadata_version = 3, .artifact_id = sparse_ref.artifact_id, .checksum = sparse_ref.checksum, .byte_len = sparse_ref.byte_len } }};
    const Harness = struct {
        a: A,
        state: lifecycle.State,
        fn read(raw: *anyopaque, alloc: A, _: u64, _: local.api_operation.RequestContext) ![]u8 {
            return lifecycle.encode(alloc, @as(*@This(), @ptrCast(@alignCast(raw))).state);
        }
        fn mutate(raw: *anyopaque, _: u64, revision: u64, mutation: lifecycle.Mutation, _: local.api_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (revision != self.state.revision) return error.CatalogGenerationChanged;
            self.state = try mutation.apply(self.a, self.state);
        }
        fn collect(self: *@This(), store_: stores.ArtifactStore, identity_: [32]u8, token: [16]u8) !void {
            for (0..64) |_| {
                var collector: gc.Collector = .{ .a = std.testing.allocator, .table = 1, .authority = .{ .ptr = self, .context = .{}, .read = read, .mutate = mutate }, .store = store_, .identity = identity_, .context = .{}, .token = token, .options = .{ .dry_run = false, .max_marked = 1, .max_deleted = 1 } };
                if ((try collector.run()).complete) return;
            }
            return error.TestUnexpectedResult;
        }
    };
    var harness: Harness = .{ .a = ca, .state = try (lifecycle.State{}).synchronize(ca, .{ .generation = 1, .published = publication }) };
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    harness.state = try harness.state.acquire(ca, @splat(9), 1, now);
    store.upload_scope = try stores.UploadScope.forPublication(domain, 2, std.testing.io);
    var next = navigation;
    next.delete_proof = null;
    next.snapshot = "second";
    var next_ref = try store.put(try std.json.Stringify.valueAlloc(ca, next, .{}));
    defer next_ref.deinit(a);
    publication.generation = 2;
    publication.declarations = &.{};
    publication.token = store.upload_scope.?.attempt;
    publication.source_navigation = .{ .kind = .doc_values, .metadata_version = 2, .artifact_id = next_ref.artifact_id, .checksum = next_ref.checksum, .byte_len = next_ref.byte_len };
    publication.base_source.external_iceberg.snapshot_id = "second";
    harness.state = try harness.state.synchronize(ca, .{ .generation = 2, .published = publication });
    try harness.collect(store, identity, @splat(6));
    const selector_id = try page_store.PageStore.identity(domain, versions);
    const retained = try store.getAlloc(&selector_id);
    a.free(retained);
    const kept_document = try store.getAlloc(document.artifact_id);
    a.free(kept_document);
    harness.state = try harness.state.release(ca, @splat(9));
    try harness.collect(store, identity, @splat(8));
    try std.testing.expectError(error.FileNotFound, store.getAlloc(&selector_id));
    try std.testing.expectError(error.FileNotFound, store.getAlloc(document.artifact_id));
}
