// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
};
pub const Job = union(enum) {
    publication: local.metadata_lake_index_catalog.Publication,
    artifact: Ref,
    contribution_page: Ref,
    chunk: artifacts.ChunkRef,
    page: struct { ref: tree.Ref, ordered_rows: bool, contributions: bool = false },
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
            .contribution_page => |page| {
                if (!try self.expand(.{ .artifact_id = page.artifact_id, .checksum = page.checksum, .byte_len = page.byte_len }, "contributions")) return;
                try stores.chargeReadBudget(&collector.remaining_reads, page.byte_len);
                for (try @import("lake_index_directory.zig").loadContributionPage(a, collector.store, page, collector.cancellation(), null)) |contribution| try self.enqueue(.{ .artifact = contribution.artifact });
            },
            .publication => |publication| {
                try self.enqueue(.{ .chunk = .{ .artifact_id = publication.inventory.artifact_id, .checksum = publication.inventory.checksum, .byte_len = publication.inventory.byte_len } });
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
                if (!try self.expand(.{ .artifact_id = &try page_store.PageStore.identity(self.value.domain, page), .checksum = &std.fmt.bytesToHex(&page.digest, .lower), .byte_len = page.bytes }, if (job_page.contributions) "contribution-index" else if (job_page.ordered_rows) "ordered-page" else "page")) return;
                var write_bytes: u64 = 0;
                var pages: page_store.PageStore = .{ .domain = self.value.domain, .artifacts = &collector.store, .cancellation = collector.cancellation(), .remaining_read_bytes = &collector.remaining_reads, .remaining_write_bytes = &write_bytes };
                const Visitor = struct {
                    progress: *Progress,
                    ordered: bool,
                    contributions: bool,
                    a: A,
                    pub fn child(visitor: *@This(), ref: tree.Ref) !void {
                        try visitor.progress.enqueue(.{ .page = .{ .ref = ref, .ordered_rows = visitor.ordered, .contributions = visitor.contributions } });
                    }
                    pub fn record(visitor: *@This(), key: []const u8, bytes: []const u8) !void {
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
                var visitor: Visitor = .{ .progress = self, .ordered = job_page.ordered_rows, .contributions = job_page.contributions, .a = a };
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
        return .{ .kind = .external_base_source, .artifact_id = upload.artifact_id, .checksum = upload.checksum, .byte_len = upload.byte_len };
    }
};
pub fn run(collector: *gc.Collector, state: lifecycle.State) !gc.Result {
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
    if (collection.progress == null) for (state.publications) |publication| {
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
        try collector.store.delete(id);
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
    try collector.authority.apply(collector.table, .{ .finish_collection = .{ .token = collector.token, .now_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms } });
    // Legacy nonce staging predates journal-discoverable pending-v2 files.
    // Its advisory migration cleanup must never gate collection completion.
    collector.store.cleanupRetiredScopedTemporaryRange(domain, collector.upload_floor, collector.upload_cutoff, collector.cancellation()) catch |err| {
        std.log.warn("completed lake GC legacy staging cleanup deferred: {t}", .{err});
    };
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
