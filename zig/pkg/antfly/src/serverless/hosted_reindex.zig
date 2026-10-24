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

//! Resumable hosted index rebuilds. Immutable checkpoints are published through
//! a pointer CAS guarded atomically by the live native HEAD lease record.
const std = @import("std");
const objects = @import("objectstore");
const manifests = @import("manifest/mod.zig");
const artifacts = @import("artifacts/store.zig");
const builder = @import("build/builder.zig");
const facts = @import("build/document_facts.zig");
const fact_builder = @import("build/document_facts_builder.zig");
const materializer = @import("query/materializer.zig");
const tree = @import("graph_segment/page_tree.zig");
const PageStore = @import("graph_segment/page_store.zig").PageStore;
const lease = @import("build/work_lease.zig");
const A = std.mem.Allocator;
const ttl = 10 * std.time.ns_per_min;
pub const State = struct {
    format: u8 = 1,
    id: []const u8,
    source_version: u64,
    source_facts: manifests.ArtifactRef,
    configuration: [32]u8,
    after: []const u8 = "",
    scanned: u64 = 0,
    phase: enum { scan, catchup, publish } = .scan,
    candidate: ?u64 = null,
    ack_cursor: ?u64 = null,
    target_lsn: ?u64 = null,
    staged: manifests.Manifest,
};
pub const Pointer = struct {
    format: u8 = 1,
    expires: u64,
    artifact: manifests.ArtifactRef,
};
pub const Saved = struct { etag: ?[]const u8 = null, pointer: ?Pointer = null, state: ?State = null };
pub const Registry = struct {
    client: objects.Client,
    bucket: []const u8,
    prefix: []const u8,
    namespace: []const u8,
    store: *@import("artifacts/mod.zig").ArtifactStore,
    pub fn key(self: Registry, a: A) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/progress/{s}/REINDEX", .{ self.prefix, self.namespace });
    }
    /// Small metadata only: callers use a metadata arena, not the allocator
    /// used by the index builder's large, promptly-freed temporary payloads.
    pub fn load(self: Registry, a: A, now: u64) !Saved {
        var client = self.client;
        client.allocator = a;
        const k = try self.key(a);
        var value = client.getObject(self.bucket, k, .{}) catch |err| switch (err) {
            error.FileNotFound => return .{},
            else => return err,
        };
        defer value.deinit(a);
        const etag = try a.dupe(u8, value.metadata.etag orelse return error.ConditionalReadUnsupported);
        if (value.body.len == 0) return .{ .etag = etag };
        if (value.body.len > 4096) return error.InvalidReindexCheckpoint;
        const pointer = (try std.json.parseFromSlice(Pointer, a, value.body, .{ .allocate = .alloc_always })).value;
        if (pointer.format != 1 or pointer.artifact.byte_len > 1024 * 1024) return error.InvalidReindexCheckpoint;
        if (pointer.expires <= now) return .{ .etag = etag };
        const state = try self.readState(a, pointer.artifact);
        return .{ .etag = etag, .pointer = pointer, .state = state };
    }
    pub fn readState(self: Registry, a: A, ref: manifests.ArtifactRef) !State {
        if (ref.byte_len > 1024 * 1024) return error.InvalidReindexCheckpoint;
        const body = try self.store.getVerifiedAllocWithCancellationUsingAllocator(a, ref.artifact_id, ref.byte_len, ref.checksum, .none);
        defer a.free(body);
        const state = (try std.json.parseFromSlice(State, a, body, .{ .allocate = .alloc_always })).value;
        if (state.format != 1 or state.id.len != 32 or !std.mem.eql(u8, self.namespace, state.staged.namespace)) return error.InvalidReindexCheckpoint;
        return state;
    }
    pub fn save(self: Registry, a: A, previous: ?[]const u8, state: ?State, held: *lease.HeldLease) !void {
        try held.renew(held.ttl_ns);
        const body = if (state) |checkpoint| bytes: {
            const encoded = try std.json.Stringify.valueAlloc(a, checkpoint, .{});
            if (encoded.len > 1024 * 1024) return error.InvalidReindexCheckpoint;
            var metadata = try self.store.put(encoded);
            defer metadata.deinit(self.store.allocator);
            break :bytes try std.json.Stringify.valueAlloc(a, Pointer{ .expires = lease.nowUnixNs(held.io) +| ttl, .artifact = .{ .kind = .stored_fields, .artifact_id = metadata.artifact_id, .checksum = metadata.checksum, .byte_len = metadata.byte_len } }, .{});
        } else "";
        const head_key = try std.fmt.allocPrint(a, "{s}/progress/{s}/HEAD", .{ self.prefix, self.namespace });
        var client = self.client;
        client.allocator = a;
        var head = try client.statObject(self.bucket, head_key);
        defer head.deinit(a);
        const record = (try @import("head_coordination.zig").parseContentTypeAlloc(a, head.content_type)) orelse return error.WorkLeaseLost;
        if (record.value.released or record.value.fencing_token != held.acquisition.fencing_token or record.value.owner_id == null or !std.mem.eql(u8, record.value.owner_id.?, held.owner_id)) return error.WorkLeaseLost;
        var result = try client.putObject(self.bucket, try self.key(a), body, .{ .if_match_etag = previous, .if_none_match = previous == null, .guard = .{ .key = head_key, .etag = head.etag orelse return error.ConditionalReadUnsupported } });
        result.deinit(a);
    }
};
pub const Action = enum { start, step, status, cancel };
pub const Context = struct {
    alloc: A,
    registry: Registry,
    manifests: *manifests.ManifestStore,
    progress: *@import("catalog/mod.zig").ProgressStore,
    wal: *@import("wal/mod.zig").WalStore,
    io: std.Io,
    cancellation: @import("maintenance_cancellation.zig").Token,

    pub fn run(self: Context, action: Action, job_id: ?[]const u8, records: usize) ![]u8 {
        if (records == 0 or records > 64) return error.InvalidReindexBudget;
        var metadata = std.heap.ArenaAllocator.init(self.alloc);
        defer metadata.deinit();
        const m = metadata.allocator();
        var saved = try self.registry.load(m, lease.nowUnixNs(self.io));
        if (action == .status) return self.response(saved.state, false);
        var nonce: [16]u8 = undefined;
        self.io.random(&nonce);
        const owner = std.fmt.bytesToHex(nonce, .lower);
        var held = (try lease.acquireHeld(try self.progress.workLeaseProvider(), self.io, self.registry.namespace, &owner, 30 * std.time.ns_per_s)) orelse return error.WorkLeaseLost;
        defer if (!held.released) {
            _ = held.release() catch false;
        };
        // Read under the same ownership epoch as the checkpoint/GC barrier.
        saved = try self.registry.load(m, lease.nowUnixNs(self.io));
        const scope = try artifacts.UploadScope.forPublication(PageStore.namespaceDomain(self.registry.namespace), held.acquisition.fencing_token, self.io);
        const previous_scope = self.registry.store.upload_scope;
        self.registry.store.upload_scope = scope;
        defer self.registry.store.upload_scope = previous_scope;
        if (action != .start and (saved.state == null or job_id == null or !std.mem.eql(u8, job_id.?, saved.state.?.id))) return self.response(null, false);
        if (action == .cancel) {
            try self.registry.save(m, saved.etag, null, &held);
            return self.response(null, false);
        }
        if (action == .start) {
            if (saved.state != null) return self.response(saved.state, false);
            const version = self.progress.getHead(self.registry.namespace) catch |err| switch (err) {
                error.FileNotFound => return self.response(null, false),
                else => return err,
            };
            var source_store = self.manifests.*;
            source_store.allocator = m;
            var source = try source_store.getAlloc(self.registry.namespace, version);
            if (source.stats.graph_segment_count != 0) return error.ReindexCapabilityUnsupported;
            if (source.base_source) |base| switch (base) {
                .antfly_document_segments => {},
                else => return error.ReindexCapabilityUnsupported,
            };
            const source_facts = findFacts(source) orelse return error.DocumentFactsRequired;
            const config = try configuration(m, source);
            var kept = std.ArrayList(manifests.ArtifactRef).empty;
            for (source.artifacts) |ref| switch (ref.kind) {
                .text_segment, .vector_segment, .sparse_segment => {},
                .document_segment, .mutation_segment, .document_facts => try kept.append(m, ref),
                else => return error.ReindexCapabilityUnsupported,
            };
            source.artifacts = try kept.toOwnedSlice(m);
            source.stats.text_segment_count = 0;
            source.stats.vector_segment_count = 0;
            source.stats.sparse_segment_count = 0;
            saved.state = .{ .id = try m.dupe(u8, &owner), .source_version = version, .source_facts = source_facts, .configuration = config, .staged = source };
            try self.registry.save(m, saved.etag, saved.state, &held);
            return self.response(saved.state, false);
        }
        var state = saved.state.?;
        var read_bytes: u64 = 32 * 1024 * 1024;
        var write_bytes: u64 = 32 * 1024 * 1024;
        var bridge = @import("maintenance_cancellation.zig").GraphBridge{ .maintenance = self.cancellation };
        var pages = PageStore{ .domain = scope.domain, .attempt = scope.attempt, .artifacts = self.registry.store, .cancellation = bridge.token(), .remaining_read_bytes = &read_bytes, .remaining_write_bytes = &write_bytes };
        const version = try self.progress.getHead(self.registry.namespace);
        var live = try self.manifests.getAlloc(self.registry.namespace, version);
        defer live.deinit(self.alloc);
        if (!std.mem.eql(u8, &state.configuration, &try configuration(m, live))) return error.ReindexDefinitionChanged;
        if (state.candidate) |candidate| {
            var lineage: ?u64 = state.ack_cursor orelse version;
            for (0..128) |_| {
                const v = lineage orelse break;
                if (v == candidate) {
                    try self.registry.save(m, saved.etag, null, &held);
                    return self.response(null, true);
                }
                if (v < candidate) break;
                var ancestor = try self.manifests.getAlloc(self.registry.namespace, v);
                defer ancestor.deinit(self.alloc);
                lineage = if (ancestor.publication_lineage_tracked) ancestor.publication_parent_version else null;
            }
            if (lineage != null and lineage.? >= candidate) {
                state.ack_cursor = lineage;
                try self.registry.save(m, saved.etag, state, &held);
                return self.response(state, false);
            }
            state.candidate = null;
            state.ack_cursor = null;
        }
        switch (state.phase) {
            .scan => {
                const source = try facts.loadRoot(self.alloc, &pages, state.source_facts);
                var cursor = try tree.Cursor.init(self.alloc, pages.store(), source.page, state.after, null);
                defer cursor.deinit();
                var docs = std.ArrayList(materializer.Document).empty;
                defer {
                    for (docs.items) |*doc| doc.deinit(self.alloc);
                    docs.deinit(self.alloc);
                }
                var bytes: u64 = 0;
                var exhausted = false;
                while (docs.items.len < records) {
                    const row = (try cursor.next()) orelse {
                        exhausted = true;
                        break;
                    };
                    if (std.mem.eql(u8, row.key, state.after)) continue;
                    const fact = try facts.Fact.decode(row.value);
                    if (fact.body.bytes > 4 * 1024 * 1024) return error.ReindexDocumentBudgetExceeded;
                    if (bytes + fact.body.bytes > 4 * 1024 * 1024) break;
                    bytes += fact.body.bytes;
                    const id = try self.alloc.dupe(u8, row.key);
                    errdefer self.alloc.free(id);
                    const body = try facts.readBodyAlloc(self.alloc, &pages, fact.body);
                    errdefer self.alloc.free(body);
                    try docs.append(self.alloc, .{ .doc_id = id, .body = body, .last_lsn = fact.last_lsn, .last_timestamp_ns = fact.last_timestamp_ns });
                }
                if (docs.items.len != 0) {
                    const mutations = try self.alloc.alloc(materializer.Mutation, docs.items.len);
                    defer self.alloc.free(mutations);
                    for (docs.items, mutations) |doc, *mutation| mutation.* = .{ .kind = .upsert, .doc_id = doc.doc_id, .body = doc.body, .lsn = doc.last_lsn, .timestamp_ns = doc.last_timestamp_ns };
                    try self.updateIndexes(m, &state, &.{}, docs.items, mutations, scope.attempt);
                    state.after = try m.dupe(u8, docs.items[docs.items.len - 1].doc_id);
                    state.scanned += docs.items.len;
                }
                if (exhausted) state.phase = .catchup;
            },
            .catchup, .publish => {
                // Freeze a finite accepted prefix. Arrivals after that capture
                // remain in the live overlay; requiring a quiet WAL on two
                // consecutive invocations would starve under steady writes.
                const latest = @max(state.target_lsn orelse try self.wal.latestLsn(self.registry.namespace), live.wal_end_lsn);
                state.target_lsn = latest;
                if (state.staged.wal_end_lsn < latest) {
                    const end = @min(latest, state.staged.wal_end_lsn +| records);
                    const records_slice = try self.wal.readRangeAlloc(self.registry.namespace, .{ .start_lsn = state.staged.wal_end_lsn + 1, .end_lsn = end, .max_records = records, .max_bytes = 4 * 1024 * 1024 });
                    defer @import("wal/mod.zig").freeRecords(self.alloc, records_slice);
                    if (records_slice.len != end - state.staged.wal_end_lsn) return error.ReindexWalGap;
                    const mutations = try builder.decodeWalMutationsAlloc(self.alloc, records_slice);
                    defer {
                        for (mutations) |mutation| {
                            self.alloc.free(mutation.doc_id);
                            if (mutation.body) |body| self.alloc.free(body);
                        }
                        self.alloc.free(mutations);
                    }
                    const root = try facts.loadRoot(self.alloc, &pages, findFacts(state.staged) orelse return error.DocumentFactsRequired);
                    var touched = try fact_builder.materializeTouchedAlloc(self.alloc, &pages, root, mutations);
                    defer touched.deinit();
                    try self.updateIndexes(m, &state, touched.before, touched.after, mutations, scope.attempt);
                    const ref = try fact_builder.publishAlloc(self.alloc, &pages, findFacts(state.staged), touched.after, mutations, state.staged.stats.policy, state.staged.stats.indexes_json, end);
                    defer builder.freeArtifactRef(self.alloc, ref);
                    const new_root = try facts.loadRoot(self.alloc, &pages, ref);
                    try replaceFamily(m, &state.staged, .document_facts, &.{ref});
                    state.staged.stats.document_count = new_root.document_count;
                    state.staged.stats.derived_outputs = try builder.derivedOutputsFromFactCountsAlloc(m, new_root.counts);
                    state.staged.wal_end_lsn = end;
                    state.phase = .catchup;
                } else if (state.phase == .catchup) state.phase = .publish else {
                    if (state.staged.wal_end_lsn < live.wal_end_lsn) return error.ReindexWalGap;
                    const empty = try @import("document_segment/mod.zig").encodeAlloc(self.alloc, &.{});
                    defer self.alloc.free(empty);
                    var snapshot = try self.registry.store.put(empty);
                    defer snapshot.deinit(self.registry.store.allocator);
                    const snapshot_ref = try builder.artifactRefFromMetadataAlloc(self.alloc, .document_segment, snapshot);
                    defer builder.freeArtifactRef(self.alloc, snapshot_ref);
                    try replaceFamily(m, &state.staged, .document_segment, &.{snapshot_ref});
                    try replaceFamily(m, &state.staged, .mutation_segment, &.{});
                    state.staged.stats.document_publish_mode = .inline_rebase;
                    state.staged.stats.document_base_version = version + 1;
                    state.staged.version = version + 1;
                    state.staged.wal_start_lsn = state.staged.wal_end_lsn;
                    state.staged.built_at_ns = lease.nowUnixNs(self.io);
                    try builder.stampPublicationFence(&state.staged, held.guard());
                    _ = try builder.putManifestForPublication(self.manifests, &state.staged, version);
                    state.candidate = state.staged.version;
                    try self.registry.save(m, saved.etag, state, &held);
                    if (!try builder.compareAndSwapHeadGuarded(self.progress, self.registry.namespace, version, state.staged.version, held.guard())) return error.ReindexPublicationConflict;
                    // The next step acknowledges even an ambiguous HEAD response.
                    return self.response(state, true);
                }
            },
        }
        try self.registry.save(m, saved.etag, state, &held);
        return self.response(state, false);
    }

    fn updateIndexes(self: Context, m: A, state: *State, before: []const materializer.Document, docs: []const materializer.Document, mutations: []const materializer.Mutation, attempt: [16]u8) !void {
        const full_text = @import("antfly_local_sources").api_full_text_indexes;
        const specs = try builder.resolvePublishedTextIndexSpecsAlloc(self.alloc, .{ .schema_json = state.staged.stats.schema_json, .read_schema_json = state.staged.stats.read_schema_json, .indexes_json = state.staged.stats.indexes_json }, &.{});
        defer full_text.freeFullTextIndexSpecs(self.alloc, specs);
        const text = try builder.buildTextArtifactRefsForTouchedDocsAllocUntil(self.alloc, self.registry.store, state.staged, before, docs, mutations, specs, self.cancellation, true, true, attempt, self.registry.namespace);
        defer {
            builder.freeArtifactRefs(self.alloc, text);
        }
        try replaceFamily(m, &state.staged, .text_segment, text);
        const context = builder.ProjectionBuildContext{ .enabled = true, .partial = true, .namespace = self.registry.namespace, .attempt = attempt };
        const sparse = try builder.buildSparseArtifactRefsWithContextAllocUntil(self.alloc, self.registry.store, state.staged, before, docs, mutations, state.staged.stats.published_search_sources, self.cancellation, context);
        defer {
            builder.freeArtifactRefs(self.alloc, sparse);
        }
        try replaceFamily(m, &state.staged, .sparse_segment, sparse);
        const dense = try builder.buildVectorArtifactRefsWithContextAllocUntil(self.alloc, self.registry.store, state.staged, .cosine, before, docs, mutations, null, &.{}, state.staged.stats.published_search_sources, self.cancellation, context);
        defer {
            builder.freeArtifactRefs(self.alloc, dense);
        }
        try replaceFamily(m, &state.staged, .vector_segment, dense);
    }
    fn response(self: Context, state: ?State, published: bool) ![]u8 {
        return std.json.Stringify.valueAlloc(self.alloc, .{ .job_id = if (state) |s| s.id else null, .phase = if (state) |s| @tagName(s.phase) else "done", .scanned = if (state) |s| s.scanned else 0, .has_more = state != null, .published = published }, .{});
    }
};
fn findFacts(manifest: manifests.Manifest) ?manifests.ArtifactRef {
    for (manifest.artifacts) |ref| if (ref.kind == .document_facts) return ref;
    return null;
}
fn configuration(a: A, manifest: manifests.Manifest) ![32]u8 {
    return @import("paged_projection.zig").fingerprint(a, .{ .policy = manifest.stats.policy, .sources = manifest.stats.published_search_sources, .schema = manifest.stats.schema_json, .read_schema = manifest.stats.read_schema_json, .indexes = manifest.stats.indexes_json });
}
fn replaceFamily(a: A, manifest: *manifests.Manifest, kind: manifests.ArtifactKind, refs: []const manifests.ArtifactRef) !void {
    var list = std.ArrayList(manifests.ArtifactRef).empty;
    for (manifest.artifacts) |ref| if (ref.kind != kind) {
        try list.append(a, ref);
    };
    for (refs) |ref| try list.append(a, try builder.cloneArtifactRefAlloc(a, ref));
    manifest.artifacts = try list.toOwnedSlice(a);
    switch (kind) {
        .text_segment => manifest.stats.text_segment_count = @intCast(refs.len),
        .sparse_segment => manifest.stats.sparse_segment_count = @intCast(refs.len),
        .vector_segment => manifest.stats.vector_segment_count = @intCast(refs.len),
        else => {},
    }
}

test "serverless hosted reindex releases native allocations across bounded scan and WAL catchup" {
    const a = std.testing.allocator;
    var memory = objects.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    // Single-threaded contract fixture. Real cross-key atomicity is verified
    // against the coordinator's transactionSync in the workerd suite.
    var vtable = client.vtable.*;
    vtable.supports_guarded_put = true;
    vtable.put_object = struct {
        fn put(raw: *anyopaque, allocator: A, bucket: []const u8, key: []const u8, body: []const u8, options: objects.PutOptions) !objects.PutResult {
            const backing: *objects.MemoryClient = @ptrCast(@alignCast(raw));
            var original = backing.client();
            original.allocator = allocator;
            if (options.guard) |guard| {
                var head = try original.statObject(bucket, guard.key);
                defer head.deinit(allocator);
                if (head.etag == null or !std.mem.eql(u8, head.etag.?, guard.etag)) return error.PreconditionFailed;
            }
            var plain = options;
            plain.guard = null;
            return original.putObject(bucket, key, body, plain);
        }
    }.put;
    client.vtable = &vtable;
    try client.makeBucket("database");
    var object_artifacts = try @import("artifacts/object_store.zig").ObjectStore.initWithClient(a, client, "database", "test/artifacts");
    var store = object_artifacts.artifactStore();
    defer store.deinit();
    var object_manifests = try @import("manifest/object_store.zig").ObjectStore.initWithClient(a, client, "database", "test/manifests");
    var manifest_store = object_manifests.manifestStore();
    defer manifest_store.deinit();
    var object_progress = try @import("catalog/object_progress_store.zig").ObjectProgressStore.initWithClient(a, client, "database", "test/progress");
    var progress = object_progress.progressStore();
    defer progress.deinit();
    var object_wal = try @import("wal/indexed_object_store.zig").IndexedObjectStore.init(a, client, "database", "test");
    defer object_wal.deinit();
    var wal = object_wal.walStore();
    var build = builder.Builder.init(a, &store, &manifest_store, &progress, &wal);
    build.setIo(std.testing.io);
    for (0..5) |i| {
        var id_buf: [16]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buf, "doc/{d}", .{i});
        const payload = try @import("api/codec.zig").encodeMutationAlloc(a, .{ .kind = .upsert, .doc_id = id, .body = "{\"text\":\"durable\",\"embedding\":[1,0],\"sparse_embedding\":{\"durable\":1}}" });
        defer a.free(payload);
        _ = try wal.append("docs", 1, payload);
    }
    var built = try build.publishNamespaceWithMetricAndPlanWithCancellation("docs", .cosine, .{ .targets = .{ .published_search_sources = @import("search_sources.zig").defaultPublishedSearchSources(), .include_graph = false } }, .none);
    defer built.deinit(a);
    const context = Context{ .alloc = a, .registry = .{ .client = client, .bucket = "database", .prefix = "test", .namespace = "docs", .store = &store }, .manifests = &manifest_store, .progress = &progress, .wal = &wal, .io = std.testing.io, .cancellation = .{ .io = std.testing.io } };
    const started = try context.run(.start, null, 2);
    defer a.free(started);
    const parsed = try std.json.parseFromSlice(struct { job_id: []const u8 }, a, started, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const partial = try context.run(.step, parsed.value.job_id, 2);
    a.free(partial);
    const edit = try @import("api/codec.zig").encodeMutationAlloc(a, .{ .kind = .delete, .doc_id = "doc/0" });
    defer a.free(edit);
    _ = try wal.append("docs", 2, edit);
    var finished = false;
    for (0..16) |_| {
        const result = try context.run(.step, parsed.value.job_id, 2);
        defer a.free(result);
        const status = try std.json.parseFromSlice(struct { has_more: bool }, a, result, .{ .ignore_unknown_fields = true });
        defer status.deinit();
        if (!status.value.has_more) {
            finished = true;
            break;
        }
    }
    try std.testing.expect(finished);
    var head = try manifest_store.getAlloc("docs", try progress.getHead("docs"));
    defer head.deinit(a);
    try std.testing.expectEqual(@as(u64, 4), head.stats.document_count);
    try std.testing.expect(builder.allArtifactsPagedOrAbsent(head, .vector_segment));
    try std.testing.expect(builder.allArtifactsPagedOrAbsent(head, .sparse_segment));
}
